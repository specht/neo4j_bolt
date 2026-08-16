# frozen_string_literal: true

require "json"
require "securerandom"
require "set"
require "thread"
require "neo4j/driver"
require "neo4j_bolt/version"

module Neo4jBolt
  CONSTRAINT_INDEX_PREFIX = "neo4j_bolt_"
  LOAD_INITIAL_BATCH_SIZE = 5_000
  LOAD_MEMORY_ERROR_CODES = %w[
    Neo.ClientError.General.TransactionOutOfMemoryError
    Neo.TransientError.General.MemoryPoolOutOfMemoryError
    Neo.TransientError.General.TransactionMemoryLimit
  ].freeze
  LOAD_MEMORY_GQL_STATUSES = %w[51N72 51N73].freeze
  LOAD_PROGRESS_STEP = 5_000
  DUMP_PROGRESS_STEP = 1_000
  private_constant :LOAD_MEMORY_ERROR_CODES, :LOAD_MEMORY_GQL_STATUSES,
                   :LOAD_PROGRESS_STEP, :DUMP_PROGRESS_STEP

  class ProgressReporter
    UPDATE_INTERVAL = 0.5
    BAR_WIDTH = 24
    SPINNER = %w[⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏].freeze
    ANSI = {
      reset: "\e[0m",
      bold: "\e[1m",
      dim: "\e[2m",
      red: "\e[31m",
      green: "\e[32m",
      yellow: "\e[33m",
      blue: "\e[34m",
      magenta: "\e[35m",
      cyan: "\e[36m",
      white: "\e[37m",
      bright_red: "\e[91m",
      bright_green: "\e[92m",
      bright_yellow: "\e[93m",
      bright_blue: "\e[94m",
      bright_magenta: "\e[95m",
      bright_cyan: "\e[96m",
      bright_white: "\e[97m"
    }.freeze
    ACCENT_STYLES = {
      "red" => :red, "green" => :green, "yellow" => :yellow, "blue" => :blue,
      "magenta" => :magenta, "cyan" => :cyan, "white" => :white,
      "bright-red" => :bright_red, "bright-green" => :bright_green,
      "bright-yellow" => :bright_yellow, "bright-blue" => :bright_blue,
      "bright-magenta" => :bright_magenta, "bright-cyan" => :bright_cyan,
      "bright-white" => :bright_white
    }.freeze

    def initialize(io, color: "cyan")
      @io = io
      @accent_style = ACCENT_STYLES.fetch(color.to_s) do
        raise ArgumentError, "progress color must be one of: #{ACCENT_STYLES.keys.join(', ')}"
      end
      mode = ENV.fetch("NEO4J_BOLT_PROGRESS", "auto")
      unless %w[auto pretty plain].include?(mode)
        raise ArgumentError, "NEO4J_BOLT_PROGRESS must be auto, pretty, or plain"
      end

      detected_tty = io && io.respond_to?(:tty?) && io.tty? && Neo4jBolt.bolt_verbosity.to_i.zero?
      @pretty = io && (mode == "pretty" || (mode == "auto" && detected_tty))
      @color = @pretty && !ENV.key?("NO_COLOR") && (mode == "pretty" || ENV["TERM"] != "dumb")
      @last_update_at = nil
      @line_open = false
      @spinner_index = 0
    end

    def update(message, force: false, percent: nil)
      return unless @io

      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      return if !force && @last_update_at && now - @last_update_at < UPDATE_INTERVAL

      if @pretty
        percent ||= extract_percent(message)
        render_line(activity_line(message, percent))
      else
        @io.puts message
      end
      @last_update_at = now
    end

    def note(message, level: :info)
      return unless @io

      if @pretty && level == :info
        update(message, force: true)
      else
        clear_line
        if @pretty
          icon = level == :warning ? color(:yellow, "!") : color(:cyan, "•")
          @io.puts "#{icon} #{terminal_text(message)}"
        else
          @io.puts message
        end
        @last_update_at = nil
      end
    end

    def finish(message)
      return unless @io

      if @pretty
        clear_line
        label, detail = terminal_parts(message)
        line = "#{color(:green, '✓')} #{color(:bold, label)}"
        line += "  #{detail}" unless detail.empty?
        @io.puts line
        @line_open = false
      else
        @io.puts message
      end
      @last_update_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def close
      return unless @io && @pretty && @line_open

      clear_line
    end

    private

    def activity_line(message, percent)
      label, detail = terminal_parts(message, strip_percent: !percent.nil?)
      spinner = SPINNER[@spinner_index % SPINNER.length]
      @spinner_index += 1

      parts = [color(@accent_style, spinner), color(:bold, label)]
      if percent
        percent = [[percent.to_i, 0].max, 100].min
        parts << progress_bar(percent)
        parts << color(:bold, format("%3d%%", percent))
      end
      parts << detail unless detail.empty?
      parts.join("  ")
    end

    def progress_bar(percent)
      filled = (percent * BAR_WIDTH / 100.0).round
      empty = BAR_WIDTH - filled
      "#{color(@accent_style, '━' * filled)}#{color(:dim, '─' * empty)}"
    end

    def terminal_parts(message, strip_percent: false)
      text = message.sub(/\.\.\.\z/, "")
      label, detail = text.split(": ", 2)
      label = pretty_label(label)
      detail ||= ""
      if strip_percent
        detail = detail.sub(/\s+\(\d+%\)/, "")
        detail = "" if detail.match?(/\A\d+%\z/)
      end
      detail = detail.gsub(/ \[batch (\d+)\]/) do
        " #{color(:dim, '·')} batch #{format_count(Regexp.last_match(1))}"
      end
      detail = format_counts(detail)
      detail = detail.gsub(", ", " #{color(:dim, '·')} ")
      [label, detail]
    end

    def terminal_text(message)
      label, detail = terminal_parts(message)
      detail.empty? ? format_counts(label) : "#{label}: #{detail}"
    end

    def pretty_label(label)
      case label
      when "Building temporary relationship lookup index"
        "Building lookup index"
      when "Temporary relationship lookup index ready."
        "Lookup index ready"
      when "Cleaning temporary load metadata"
        "Cleaning load metadata"
      when "Temporary load metadata removed."
        "Load metadata removed"
      else
        label
      end
    end

    def extract_percent(message)
      if (match = message.match(/\((\d+)%\)/)) || (match = message.match(/:\s*(\d+)%\z/))
        return match[1].to_i
      end

      match = message.match(/:\s*(\d+)\/(\d+) nodes\z/)
      return nil unless match

      total = match[2].to_i
      total.zero? ? 100 : match[1].to_i * 100 / total
    end

    def format_counts(text)
      text.gsub(/\b\d{4,}\b/) { |number| format_count(number) }
    end

    def format_count(number)
      number.to_s.reverse.scan(/.{1,3}/).join(",").reverse
    end

    def render_line(line)
      @io.print "\r\e[2K#{line}"
      @io.flush
      @line_open = true
    end

    def clear_line
      return unless @pretty && @line_open

      @io.print "\r\e[2K"
      @io.flush
      @line_open = false
    end

    def color(style, text)
      return text unless @color

      "#{ANSI.fetch(style)}#{text}#{ANSI[:reset]}"
    end
  end
  private_constant :ProgressReporter
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
          authentication_token
        )
      end

      def authentication_token
        username = Neo4jBolt.bolt_username
        password = Neo4jBolt.bolt_password
        return Neo4j::Driver::AuthTokens.none if username.nil? && password.nil?

        if username.nil? || password.nil?
          raise Error, "Neo4j username and password must either both be set or both be unset"
        end

        Neo4j::Driver::AuthTokens.basic(username, password)
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
    attr_reader :bolt_host, :bolt_port, :bolt_username, :bolt_password
    attr_accessor :bolt_verbosity, :bolt_database

    def bolt_host=(host)
      reconfigure_driver if defined?(@bolt_host) && @bolt_host != host
      @bolt_host = host
    end

    def bolt_port=(port)
      reconfigure_driver if defined?(@bolt_port) && @bolt_port != port
      @bolt_port = port
    end

    def bolt_username=(username)
      reconfigure_driver if defined?(@bolt_username) && @bolt_username != username
      @bolt_username = username
    end

    def bolt_password=(password)
      reconfigure_driver if defined?(@bolt_password) && @bolt_password != password
      @bolt_password = password
    end

    private

    def reconfigure_driver
      DriverRegistry.cleanup
    end
  end

  self.bolt_host = "localhost"
  self.bolt_port = 7687
  self.bolt_verbosity = 0
  self.bolt_username = ENV["NEO4J_USERNAME"]
  self.bolt_password = ENV["NEO4J_PASSWORD"]
  self.bolt_database = ENV["NEO4J_DATABASE"]

  # Keep both `include Neo4jBolt` and direct module-style calls working.
  extend self

  def with_session(driver)
    database = Neo4jBolt.bolt_database
    if database.nil?
      driver.session { |session| yield session }
    else
      driver.session(database: database) { |session| yield session }
    end
  end
  private :with_session

  def neo4j_query(query, data = {}, &block)
    validate_parameters!(data)
    log_query(query, data)

    context = transaction_context
    if context
      run_and_convert(context.transaction, query, data, &block)
    else
      DriverRegistry.with_driver do |driver|
        with_session(driver) do |session|
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
      with_session(driver) do |session|
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

  def dump_database(io, progress_io: nil, progress_color: "cyan")
    progress = ProgressReporter.new(progress_io, color: progress_color)
    dumped_nodes = 0
    dumped_relationships = 0

    transaction do
      identity_function = dump_identity_function
      total_nodes = neo4j_query_expect_one("MATCH (n) RETURN count(n) AS count")["count"]
      total_relationships = neo4j_query_expect_one("MATCH ()-[r]->() RETURN count(r) AS count")["count"]
      dump_ids = total_relationships.positive? ? {} : nil

      progress.update(
        progress_message("Dumping", dumped_nodes, dumped_relationships, total_nodes, total_relationships),
        force: true
      )

      neo4j_query(
        "MATCH (n) " \
        "RETURN #{identity_function}(n) AS identity, labels(n) AS labels, properties(n) AS properties " \
        "ORDER BY #{identity_function}(n)"
      ) do |row|
        dump_ids[row["identity"].to_s] = dumped_nodes if dump_ids
        io.puts "n #{JSON.generate(id: dumped_nodes, labels: row["labels"], properties: row["properties"])}"
        dumped_nodes += 1
        if (dumped_nodes % DUMP_PROGRESS_STEP).zero?
          progress.update(progress_message(
            "Dumping", dumped_nodes, dumped_relationships, total_nodes, total_relationships
          ))
        end
      end

      if total_relationships.positive?
        neo4j_query(
          "MATCH (from)-[r]->(to) " \
          "RETURN #{identity_function}(from) AS from_identity, " \
          "#{identity_function}(to) AS to_identity, type(r) AS type, properties(r) AS properties " \
          "ORDER BY #{identity_function}(r)"
        ) do |row|
          io.puts "r #{JSON.generate(
            from: dump_ids.fetch(row["from_identity"].to_s),
            to: dump_ids.fetch(row["to_identity"].to_s),
            type: row["type"],
            properties: row["properties"]
          )}"
          dumped_relationships += 1
          if (dumped_relationships % DUMP_PROGRESS_STEP).zero?
            progress.update(progress_message(
              "Dumping", dumped_nodes, dumped_relationships, total_nodes, total_relationships
            ))
          end
        end
      end
    end

    progress.finish("Dumped: #{dumped_nodes} nodes, #{dumped_relationships} relationships")
    nil
  ensure
    progress&.close
  end

  def load_database_dump(io, force_append: false, progress_io: nil, progress_color: "cyan",
                         initial_batch_size: LOAD_INITIAL_BATCH_SIZE)
    raise Error, "load_database_dump cannot run inside a transaction" if transaction_context

    initial_batch_size = Integer(initial_batch_size)
    raise ArgumentError, "initial_batch_size must be positive" unless initial_batch_size.positive?

    unless force_append
      count = neo4j_query_expect_one("MATCH (n) RETURN count(n) AS count")["count"]
      raise Error, "There are nodes in this database, exiting now." unless count.zero?
    end

    progress = ProgressReporter.new(progress_io, color: progress_color)
    node_batches = Hash.new { |hash, key| hash[key] = [] }
    relationship_batches = Hash.new { |hash, key| hash[key] = [] }
    total_nodes, total_relationships = parse_dump(io, node_batches, relationship_batches, progress)

    temporary_token = SecureRandom.hex(12)
    temporary_label = "__neo4j_bolt_load_#{temporary_token}"
    temporary_id_property = "__neo4j_bolt_load_id_#{temporary_token}"
    temporary_index = "neo4j_bolt_load_#{temporary_token}"
    quoted_temporary_label = quote_identifier(temporary_label)
    quoted_temporary_id = quote_identifier(temporary_id_property)
    quoted_temporary_index = quote_identifier(temporary_index)
    needs_relationship_lookup = total_relationships.positive?
    loaded_nodes = 0
    loaded_relationships = 0
    index_created = false

    progress.update(
      progress_message("Loading", loaded_nodes, loaded_relationships, total_nodes, total_relationships,
                       batch_size: initial_batch_size),
      force: true
    )

    original_error = nil
    cleanup_error = nil
    begin
      node_batch_size = initial_batch_size
      node_batches.each_value do |nodes|
        node_batch_size = adaptive_each_slice(nodes, node_batch_size, progress, "node") do |slice, current_batch_size|
          labels = slice.first.fetch("labels")
          label_clause = labels.map { |label| ":#{quote_identifier(label)}" }.join
          label_clause += ":#{quoted_temporary_label}" if needs_relationship_lookup
          temporary_assignment = if needs_relationship_lookup
                                   "SET n.#{quoted_temporary_id} = item.id\n"
                                 else
                                   ""
                                 end
          count = neo4j_query_expect_one(<<~CYPHER, nodes: slice)["count_n"]
            UNWIND $nodes AS item
            CREATE (n#{label_clause})
            SET n = item.properties
            #{temporary_assignment}RETURN count(n) AS count_n
          CYPHER
          raise Error, "Expected #{slice.size} nodes, got #{count}." unless count == slice.size

          loaded_nodes += slice.size
          progress.update(progress_message(
            "Loading", loaded_nodes, loaded_relationships, total_nodes, total_relationships,
            batch_size: current_batch_size
          ))
        end
      end

      if needs_relationship_lookup
        progress.note("Building temporary relationship lookup index...")
        neo4j_query(
          "CREATE INDEX #{quoted_temporary_index} " \
          "FOR (n:#{quoted_temporary_label}) ON (n.#{quoted_temporary_id})"
        )
        index_created = true
        wait_for_load_index(temporary_index, progress)
        progress.update(
          progress_message("Loading", loaded_nodes, loaded_relationships, total_nodes, total_relationships,
                           batch_size: initial_batch_size),
          force: true
        )

        relationship_batch_size = initial_batch_size
        relationship_batches.each do |type, relationships|
          relationship_batch_size = adaptive_each_slice(
            relationships, relationship_batch_size, progress, "relationship"
          ) do |slice, current_batch_size|
            count = neo4j_query_expect_one(<<~CYPHER, relationships: slice)["count_r"]
              UNWIND $relationships AS item
              MATCH (from:#{quoted_temporary_label} {#{quoted_temporary_id}: item.from})
              MATCH (to:#{quoted_temporary_label} {#{quoted_temporary_id}: item.to})
              CREATE (from)-[r:#{quote_identifier(type)}]->(to)
              SET r = item.properties
              RETURN count(r) AS count_r
            CYPHER
            raise Error, "Expected #{slice.size} relationships, got #{count}." unless count == slice.size

            loaded_relationships += slice.size
            progress.update(progress_message(
              "Loading", loaded_nodes, loaded_relationships, total_nodes, total_relationships,
              batch_size: current_batch_size
            ))
          end
        end
      end
    rescue Exception => error # rubocop:disable Lint/RescueException -- preserve original failure through cleanup
      original_error = error
      raise
    ensure
      if index_created
        begin
          neo4j_query("DROP INDEX #{quoted_temporary_index} IF EXISTS")
        rescue Error => error
          cleanup_error ||= error
        end
      end

      if needs_relationship_lookup
        begin
          progress&.note("Cleaning temporary load metadata...")
          cleanup_load_metadata(
            quoted_temporary_label, quoted_temporary_id, initial_batch_size, total_nodes, progress
          )
        rescue Error => error
          cleanup_error ||= error
        end
      end

      raise cleanup_error if original_error.nil? && cleanup_error
    end

    progress.finish("Loaded: #{loaded_nodes} nodes, #{loaded_relationships} relationships")
    nil
  ensure
    progress&.close
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

  def parse_dump(io, node_batches, relationship_batches, progress = nil)
    node_count = 0
    relationship_count = 0
    progress&.update("Reading dump: 0 nodes, 0 relationships", force: true)

    io.each_line.with_index(1) do |line, line_number|
      line = line.strip
      next if line.empty?

      kind, json = line.split(" ", 2)
      raise Error, "Invalid dump entry on line #{line_number}" unless json && %w[n r].include?(kind)

      value = JSON.parse(json)
      if kind == "n"
        labels = value.fetch("labels")
        node_batches[labels.sort] << value
        node_count += 1
      else
        relationship_batches[value.fetch("type")] << value
        relationship_count += 1
      end
      if ((node_count + relationship_count) % LOAD_PROGRESS_STEP).zero?
        progress&.update("Reading dump: #{node_count} nodes, #{relationship_count} relationships")
      end
    rescue JSON::ParserError, KeyError => error
      raise Error, "Invalid dump entry on line #{line_number}: #{error.message}"
    end

    progress&.finish("Read dump: #{node_count} nodes, #{relationship_count} relationships")
    [node_count, relationship_count]
  end

  def adaptive_each_slice(items, batch_size, progress, kind)
    offset = 0
    while offset < items.size
      slice = items.slice(offset, [batch_size, items.size - offset].min)
      begin
        yield slice, batch_size
        offset += slice.size
      rescue Error => error
        raise unless load_batch_memory_error?(error) && slice.size > 1

        reduced_batch_size = [slice.size / 2, 1].max
        batch_size = [batch_size, reduced_batch_size].min
        progress&.note(
          "Neo4j rejected a #{kind} batch of #{slice.size} for transaction memory; " \
          "retrying with batches of #{batch_size}.",
          level: :warning
        )
      end
    end
    batch_size
  end

  def load_batch_memory_error?(error)
    upstream = error.cause
    return false unless upstream

    LOAD_MEMORY_ERROR_CODES.include?(upstream.respond_to?(:code) ? upstream.code : nil) ||
      LOAD_MEMORY_GQL_STATUSES.include?(upstream.respond_to?(:gql_status) ? upstream.gql_status : nil)
  end

  def wait_for_load_index(index_name, progress)
    loop do
      row = neo4j_query_expect_one(<<~CYPHER, name: index_name)
        SHOW INDEXES YIELD name, state, populationPercent
        WHERE name = $name
        RETURN state, populationPercent
      CYPHER
      state = row["state"]
      population = row["populationPercent"] || 0
      if state == "ONLINE"
        progress&.finish("Temporary relationship lookup index ready.")
        return
      end
      raise Error, "Temporary load index #{index_name} entered state #{state}." if state == "FAILED"

      progress&.update("Building temporary relationship lookup index: #{population.to_i}%")
      sleep 0.25
    end
  end

  def cleanup_load_metadata(quoted_label, quoted_id, batch_size, total_nodes, progress)
    cleaned_nodes = 0
    loop do
      begin
        count = neo4j_query_expect_one(<<~CYPHER, limit: batch_size)["count_n"]
          MATCH (n:#{quoted_label})
          WITH n LIMIT $limit
          REMOVE n.#{quoted_id}
          REMOVE n:#{quoted_label}
          RETURN count(n) AS count_n
        CYPHER
        break if count.zero?

        cleaned_nodes += count
        progress&.update("Cleaning temporary load metadata: #{cleaned_nodes}/#{total_nodes} nodes")
      rescue Error => error
        raise unless load_batch_memory_error?(error) && batch_size > 1

        batch_size = [batch_size / 2, 1].max
        progress&.note(
          "Neo4j rejected a cleanup batch for transaction memory; retrying with batches of #{batch_size}.",
          level: :warning
        )
      end
    end
    progress&.finish("Temporary load metadata removed.")
  end

  def progress_message(action, nodes, relationships, total_nodes, total_relationships, batch_size: nil)
    total = total_nodes + total_relationships
    completed = nodes + relationships
    percent = total.zero? ? 100 : (completed * 100 / total)
    message = "#{action}: #{nodes}/#{total_nodes} nodes, " \
              "#{relationships}/#{total_relationships} relationships (#{percent}%)"
    message += " [batch #{batch_size}]" if batch_size
    message
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
