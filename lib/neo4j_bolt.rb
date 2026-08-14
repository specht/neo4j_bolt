# frozen_string_literal: true

require "json"
require "securerandom"
require "set"
require "thread"
require "neo4j/driver"
require "neo4j_bolt/version"

module Neo4jBolt
  CONSTRAINT_INDEX_PREFIX = "neo4j_bolt_"
  MIN_INTEGER = -(2**63)
  MAX_INTEGER = (2**63) - 1
  TRANSACTION_CONTEXT_KEY = :neo4j_bolt_transaction_contexts

  class Error < StandardError; end
  class IntegerOutOfRangeError < Error; end
  class SyntaxError < Error; end
  class ConstraintValidationFailedError < Error; end
  class ExpectedOneResultError < Error; end

  class Node < Hash
    attr_reader :id, :labels, :element_id

    def initialize(id, labels, properties, element_id: nil)
      @id = id
      @labels = labels
      @element_id = element_id
      properties.each_pair { |key, value| self[key] = value }
    end
  end

  class Relationship < Hash
    attr_reader :id, :start_node_id, :end_node_id, :type, :element_id,
                :start_node_element_id, :end_node_element_id

    def initialize(id, start_node_id, end_node_id, type, properties, element_id: nil,
                   start_node_element_id: nil, end_node_element_id: nil)
      @id = id
      @start_node_id = start_node_id
      @end_node_id = end_node_id
      @type = type
      @element_id = element_id
      @start_node_element_id = start_node_element_id
      @end_node_element_id = end_node_element_id
      properties.each_pair { |key, value| self[key] = value }
    end
  end

  TransactionContext = Struct.new(:session, :transaction, :depth, :rollback_only, keyword_init: true)

  # Owns one process-wide upstream driver and its connection pool. Leases let
  # cleanup wait for in-flight sessions without serializing independent queries.
  module DriverRegistry
    LEASE_KEY = :neo4j_bolt_driver_leases

    class << self
      def with_driver
        driver = acquire
        yield driver
      ensure
        release if driver
      end

      def cleanup
        if Thread.current.thread_variable_get(LEASE_KEY).to_i.positive?
          raise Error, "cleanup_neo4j cannot run while this thread is using Neo4j"
        end

        driver = nil
        cleanup_started = false
        mutex.synchronize do
          condition.wait(mutex) while @closing
          @closing = true
          cleanup_started = true
          condition.wait(mutex) until leases.zero?
          driver = @driver
          @driver = nil
        end

        driver&.close
      ensure
        if cleanup_started
          mutex.synchronize do
            @closing = false
            condition.broadcast
          end
        end
      end

      private

      def acquire
        driver = mutex.synchronize do
          condition.wait(mutex) while @closing
          @driver ||= build_driver
          @leases = leases + 1
          @driver
        end
        Thread.current.thread_variable_set(
          LEASE_KEY, Thread.current.thread_variable_get(LEASE_KEY).to_i + 1
        )
        driver
      end

      def release
        Thread.current.thread_variable_set(
          LEASE_KEY, Thread.current.thread_variable_get(LEASE_KEY).to_i - 1
        )
        mutex.synchronize do
          @leases = leases - 1
          condition.broadcast if leases.zero?
        end
      end

      def build_driver
        host = Neo4jBolt.bolt_host.to_s
        host = "[#{host}]" if host.include?(":") && !host.start_with?("[")
        Neo4j::Driver::GraphDatabase.driver(
          "bolt://#{host}:#{Neo4jBolt.bolt_port}",
          Neo4j::Driver::AuthTokens.none
        )
      end

      def leases
        @leases ||= 0
      end

      def mutex
        @mutex ||= Mutex.new
      end

      def condition
        @condition ||= ConditionVariable.new
      end
    end
  end

  class << self
    attr_reader :bolt_host, :bolt_port
    attr_accessor :bolt_verbosity

    def bolt_host=(host)
      reconfigure_driver if defined?(@bolt_host) && @bolt_host != host
      @bolt_host = host
    end

    def bolt_port=(port)
      reconfigure_driver if defined?(@bolt_port) && @bolt_port != port
      @bolt_port = port
    end

    private

    def reconfigure_driver
      DriverRegistry.cleanup
    end
  end

  self.bolt_host = "localhost"
  self.bolt_port = 7687
  self.bolt_verbosity = 0

  # Keep both `include Neo4jBolt` and direct module-style calls working.
  extend self

  def neo4j_query(query, data = {}, &block)
    validate_parameters!(data)
    log_query(query, data)

    context = transaction_context
    if context
      run_and_convert(context.transaction, query, data, &block)
    else
      DriverRegistry.with_driver do |driver|
        driver.session do |session|
          run_and_convert(session, query, data, &block)
        end
      end
    end
  rescue Neo4j::Driver::Exceptions::Neo4jException => error
    mark_transaction_failed!
    raise_mapped_error(error)
  rescue IntegerOutOfRangeError
    mark_transaction_failed!
    raise
  rescue Exception # rubocop:disable Lint/RescueException -- preserve rollback-only on callback/local errors
    mark_transaction_failed!
    raise
  end

  def neo4j_query_expect_one(query, data = {})
    rows = neo4j_query(query, data)
    return rows.first if rows.size == 1

    mark_transaction_failed!
    raise ExpectedOneResultError, "Expected one result, but got #{rows.size}."
  end

  def transaction
    context = transaction_context
    if context
      context.depth += 1
      begin
        yield
      rescue Exception # rubocop:disable Lint/RescueException -- rollback-only must survive every unwind
        context.rollback_only = true
        raise
      ensure
        context.depth -= 1
      end
      return nil
    end

    DriverRegistry.with_driver do |driver|
      driver.session do |session|
        upstream_transaction = session.begin_transaction
        context = TransactionContext.new(
          session: session, transaction: upstream_transaction, depth: 1, rollback_only: false
        )
        transaction_contexts[self.object_id] = context

        begin
          yield
        rescue Exception # rubocop:disable Lint/RescueException -- ensure rollback for non-StandardError too
          context.rollback_only = true
          raise
        ensure
          begin
            context.rollback_only ? upstream_transaction.rollback : upstream_transaction.commit
          rescue Neo4j::Driver::Exceptions::Neo4jException => error
            raise_mapped_error(error)
          ensure
            transaction_contexts.delete(self.object_id)
          end
        end
      end
    end
    nil
  rescue Neo4j::Driver::Exceptions::Neo4jException => error
    raise_mapped_error(error)
  end

  # Explicit rollback is rollback-only: the surrounding block may finish, but
  # its one real upstream transaction can no longer commit.
  def rollback
    context = transaction_context
    raise Error, "rollback called outside a transaction" unless context

    context.rollback_only = true
    nil
  end

  def cleanup_neo4j
    DriverRegistry.cleanup
    nil
  rescue Neo4j::Driver::Exceptions::Neo4jException => error
    raise_mapped_error(error)
  end

  def wait_for_neo4j
    attempts = Integer(ENV.fetch("NEO4J_BOLT_WAIT_ATTEMPTS", "30"))
    delay = Float(ENV.fetch("NEO4J_BOLT_WAIT_DELAY", "1"))
    last_error = nil

    attempts.times do |attempt|
      begin
        neo4j_query("RETURN 1 AS ready")
        return true
      rescue Error => error
        last_error = error
        warn "Waiting for Neo4j (attempt #{attempt + 1}/#{attempts})..." if Neo4jBolt.bolt_verbosity.to_i.positive?
        sleep delay if attempt + 1 < attempts
      end
    end

    raise(last_error || Error.new("Neo4j did not become ready"))
  end

  def setup_constraints_and_indexes(constraints, indexes)
    wanted_constraints = Set.new
    wanted_indexes = Set.new

    constraints.each do |entry|
      label, property = schema_entry(entry, "constraint")
      name = "#{CONSTRAINT_INDEX_PREFIX}#{label}_#{property}"
      wanted_constraints << name
      neo4j_query(
        "CREATE CONSTRAINT #{quote_identifier(name)} IF NOT EXISTS " \
        "FOR (n:#{quote_identifier(label)}) REQUIRE n.#{quote_identifier(property)} IS UNIQUE"
      )
    end

    indexes.each do |entry|
      label, property = schema_entry(entry, "index")
      name = "#{CONSTRAINT_INDEX_PREFIX}#{label}_#{property}"
      wanted_indexes << name
      neo4j_query(
        "CREATE INDEX #{quote_identifier(name)} IF NOT EXISTS " \
        "FOR (n:#{quote_identifier(label)}) ON (n.#{quote_identifier(property)})"
      )
    end

    neo4j_query("SHOW CONSTRAINTS").each do |row|
      name = row["name"]
      next unless name&.start_with?(CONSTRAINT_INDEX_PREFIX)
      next if wanted_constraints.include?(name)

      neo4j_query("DROP CONSTRAINT #{quote_identifier(name)}")
    end

    neo4j_query("SHOW INDEXES").each do |row|
      name = row["name"]
      next unless name&.start_with?(CONSTRAINT_INDEX_PREFIX)
      next if wanted_indexes.include?(name) || wanted_constraints.include?(name)

      neo4j_query("DROP INDEX #{quote_identifier(name)}")
    end
    nil
  end

  def dump_database(io)
    dump_ids = {}
    next_dump_id = 0
    identity_function = dump_identity_function

    neo4j_query("MATCH (n) RETURN n ORDER BY #{identity_function}(n)") do |row|
      node = row["n"]
      dump_ids[node.element_id || node.id.to_s] = next_dump_id
      io.puts "n #{JSON.generate(id: next_dump_id, labels: node.labels, properties: node)}"
      next_dump_id += 1
    end

    neo4j_query("MATCH ()-[r]->() RETURN r ORDER BY #{identity_function}(r)") do |row|
      relationship = row["r"]
      from_identity = relationship.start_node_element_id || relationship.start_node_id.to_s
      to_identity = relationship.end_node_element_id || relationship.end_node_id.to_s
      io.puts "r #{JSON.generate(
        from: dump_ids.fetch(from_identity),
        to: dump_ids.fetch(to_identity),
        type: relationship.type,
        properties: relationship
      )}"
    end
    nil
  end

  def load_database_dump(io, force_append: false)
    unless force_append
      count = neo4j_query_expect_one("MATCH (n) RETURN count(n) AS count")["count"]
      raise Error, "There are nodes in this database, exiting now." unless count.zero?
    end

    node_batches = Hash.new { |hash, key| hash[key] = [] }
    relationship_batches = Hash.new { |hash, key| hash[key] = [] }
    parse_dump(io, node_batches, relationship_batches)

    temporary_id_property = "__neo4j_bolt_load_#{SecureRandom.hex(12)}"
    quoted_temporary_id = quote_identifier(temporary_id_property)
    loaded_nodes = 0
    loaded_relationships = 0

    begin
      node_batches.each_value do |nodes|
        nodes.each_slice(256) do |slice|
          labels = slice.first.fetch("labels")
          label_clause = labels.map { |label| ":#{quote_identifier(label)}" }.join
          count = neo4j_query_expect_one(<<~CYPHER, nodes: slice)["count_n"]
            UNWIND $nodes AS item
            CREATE (n#{label_clause})
            SET n = item.properties
            SET n.#{quoted_temporary_id} = item.id
            RETURN count(n) AS count_n
          CYPHER
          raise Error, "Expected #{slice.size} nodes, got #{count}." unless count == slice.size

          loaded_nodes += slice.size
          report_load_progress(loaded_nodes, loaded_relationships)
        end
      end

      relationship_batches.each do |type, relationships|
        relationships.each_slice(256) do |slice|
          count = neo4j_query_expect_one(<<~CYPHER, relationships: slice)["count_r"]
            UNWIND $relationships AS item
            MATCH (from) WHERE from.#{quoted_temporary_id} = item.from
            MATCH (to) WHERE to.#{quoted_temporary_id} = item.to
            CREATE (from)-[r:#{quote_identifier(type)}]->(to)
            SET r = item.properties
            RETURN count(r) AS count_r
          CYPHER
          raise Error, "Expected #{slice.size} relationships, got #{count}." unless count == slice.size

          loaded_relationships += slice.size
          report_load_progress(loaded_nodes, loaded_relationships)
        end
      end
    ensure
      original_error = $!
      begin
        neo4j_query("MATCH (n) WHERE n.#{quoted_temporary_id} IS NOT NULL REMOVE n.#{quoted_temporary_id}") if loaded_nodes.positive?
      rescue Error
        raise if original_error.nil?
      end
    end

    warn if loaded_nodes.positive? || loaded_relationships.positive?
    nil
  end

  private

  def run_and_convert(runner, query, data)
    result = if runner.is_a?(Neo4j::Driver::Session)
               runner.run(query, data)
             else
               runner.run(query, **data)
             end

    if block_given?
      result.each do |record|
        row = convert_record(record)
        log_row(row)
        yield row
      end
      nil
    else
      result.map do |record|
        row = convert_record(record)
        log_row(row)
        row
      end
    end
  end

  def convert_record(record)
    record.keys.each_with_index.to_h do |key, index|
      [key.to_s, convert_value(record.values[index])]
    end
  end

  def convert_value(value)
    case value
    when Neo4j::Driver::Types::Node
      Node.new(
        value.id,
        value.labels.map(&:to_s),
        convert_property_map(value.properties),
        element_id: value.element_id
      )
    when Neo4j::Driver::Types::Relationship
      Relationship.new(
        value.id,
        value.start_node_id,
        value.end_node_id,
        value.type.to_s,
        convert_property_map(value.properties),
        element_id: value.element_id,
        start_node_element_id: value.start_node_element_id,
        end_node_element_id: value.end_node_element_id
      )
    when Hash
      convert_property_map(value)
    when Array
      value.map { |item| convert_value(item) }
    else
      value
    end
  end

  def convert_property_map(map)
    map.to_h do |key, value|
      [key.to_sym, convert_value(value)]
    end
  end

  def validate_parameters!(value)
    case value
    when Integer
      return if value.between?(MIN_INTEGER, MAX_INTEGER)

      raise IntegerOutOfRangeError, "Integer value out of signed 64-bit range: #{value}"
    when Hash
      value.each_value { |nested_value| validate_parameters!(nested_value) }
    when Array, Set
      value.each { |nested_value| validate_parameters!(nested_value) }
    end
  end

  def raise_mapped_error(error)
    mapped = case error.code
             when "Neo.ClientError.Statement.SyntaxError"
               SyntaxError.new(error.message)
             when "Neo.ClientError.Schema.ConstraintValidationFailed"
               ConstraintValidationFailedError.new(error.message)
             else
               message = [error.code, error.message].compact.join("\n")
               Error.new(message)
             end
    mapped.set_backtrace(error.backtrace)
    raise mapped, cause: error
  end

  def transaction_contexts
    Thread.current.thread_variable_get(TRANSACTION_CONTEXT_KEY) ||
      Thread.current.thread_variable_set(TRANSACTION_CONTEXT_KEY, {})
  end

  def transaction_context
    transaction_contexts[self.object_id]
  end

  def mark_transaction_failed!
    transaction_context.rollback_only = true if transaction_context
  end

  def schema_entry(entry, kind)
    parts = entry.to_s.split("/", -1)
    unless parts.size == 2 && parts.all? { |part| part.match?(/\A\w+\z/) }
      raise Error, "Unexpected #{kind} format: #{entry}"
    end
    parts
  end

  def quote_identifier(identifier)
    "`#{identifier.to_s.gsub("`", "``")}`"
  end

  # Neo4j 4.4 has no elementId() function. Its id() fallback is used only for
  # deterministic in-process ordering; those database IDs never enter a dump.
  def dump_identity_function
    version = neo4j_query_expect_one(
      "CALL dbms.components() YIELD versions RETURN versions[0] AS version LIMIT 1"
    )["version"]
    version.to_s.split(".").first.to_i >= 5 ? "elementId" : "id"
  end

  def parse_dump(io, node_batches, relationship_batches)
    io.each_line.with_index(1) do |line, line_number|
      line = line.strip
      next if line.empty?

      kind, json = line.split(" ", 2)
      raise Error, "Invalid dump entry on line #{line_number}" unless json && %w[n r].include?(kind)

      value = JSON.parse(json)
      if kind == "n"
        labels = value.fetch("labels")
        node_batches[labels.sort] << value
      else
        relationship_batches[value.fetch("type")] << value
      end
    rescue JSON::ParserError, KeyError => error
      raise Error, "Invalid dump entry on line #{line_number}: #{error.message}"
    end
  end

  def report_load_progress(nodes, relationships)
    warn "Loaded #{nodes} nodes, #{relationships} relationships..." if Neo4jBolt.bolt_verbosity.to_i.positive?
  end

  def log_query(query, data)
    return unless Neo4jBolt.bolt_verbosity.to_i >= 1

    warn query
    warn JSON.generate(data)
    warn "-" * 40
  end

  def log_row(row)
    return unless Neo4jBolt.bolt_verbosity.to_i >= 1

    warn ">>> #{row.inspect}"
    warn "-" * 40
  end
end
