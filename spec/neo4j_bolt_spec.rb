# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require "set"
require "stringio"

RSpec.describe Neo4jBolt do
  include Neo4jBolt

  class TestRollback < StandardError; end

  describe "public compatibility API" do
    it "has the prerelease version and configurable connection attributes" do
      expect(Neo4jBolt::VERSION).to eq("0.4.4")
      expect(Neo4jBolt.bolt_host).to be_a(String)
      expect(Neo4jBolt.bolt_port).to be_a(Integer)
      expect(Neo4jBolt.bolt_verbosity).to be_a(Integer)
      expect(Neo4jBolt).to respond_to(
        :bolt_username, :bolt_username=, :bolt_password, :bolt_password=,
        :bolt_database, :bolt_database=
      )
    end

    it "loads username, password, and database defaults from the standard environment variables" do
      stdout, stderr, status = Open3.capture3(
        {
          "NEO4J_USERNAME" => "env-user",
          "NEO4J_PASSWORD" => "env-password",
          "NEO4J_DATABASE" => "env-database"
        },
        RbConfig.ruby,
        "-rbundler/setup",
        "-I#{File.expand_path("../lib", __dir__)}",
        "-rneo4j_bolt",
        "-e",
        "print JSON.generate([Neo4jBolt.bolt_username, Neo4jBolt.bolt_password, Neo4jBolt.bolt_database])"
      )
      expect(status).to be_success, stderr
      expect(JSON.parse(stdout)).to eq(["env-user", "env-password", "env-database"])
    end

    it "works both as an included module and directly on Neo4jBolt" do
      expect(neo4j_query_expect_one("RETURN 1 AS value")["value"]).to eq(1)
      expect(Neo4jBolt.neo4j_query_expect_one("RETURN 2 AS value")["value"]).to eq(2)
    end

    it "keeps the documented error hierarchy" do
      expect(Neo4jBolt::IntegerOutOfRangeError).to be < Neo4jBolt::Error
      expect(Neo4jBolt::SyntaxError).to be < Neo4jBolt::Error
      expect(Neo4jBolt::ConstraintValidationFailedError).to be < Neo4jBolt::Error
      expect(Neo4jBolt::ExpectedOneResultError).to be < Neo4jBolt::Error
    end
  end

  describe "query and value compatibility" do
    it "round-trips nil and booleans" do
      [nil, false, true].each do |value|
        expect(neo4j_query_expect_one("RETURN $value AS value", value: value)["value"]).to eq(value)
      end
    end

    it "round-trips signed 64-bit integer boundaries" do
      values = [
        -9_223_372_036_854_775_808, -2_147_483_649, -2_147_483_648,
        -32_769, -32_768, -129, -128, -17, -16, -1, 0, 1, 127,
        128, 32_767, 32_768, 2_147_483_647, 2_147_483_648,
        9_223_372_036_854_775_807
      ]
      values.each do |value|
        expect(neo4j_query_expect_one("RETURN $value AS value", value: value)["value"]).to eq(value)
      end
    end

    it "maps integers outside signed 64-bit range to IntegerOutOfRangeError" do
      [-9_223_372_036_854_775_809, 9_223_372_036_854_775_808].each do |value|
        expect do
          neo4j_query("RETURN $value", value: { nested: [value] })
        end.to raise_error(Neo4jBolt::IntegerOutOfRangeError)
      end
    end

    it "round-trips floats, including infinities" do
      [1.0, Math::PI, Math.sqrt(2), Float::INFINITY, -Float::INFINITY].each do |value|
        expect(neo4j_query_expect_one("RETURN $value AS value", value: value)["value"]).to eq(value)
      end
    end

    it "round-trips UTF-8 and large strings" do
      values = ["", "🌩️ Grüße Καλημέρα", "abcdefghijklmnop" * 65_536]
      values.each do |value|
        expect(neo4j_query_expect_one("RETURN $value AS value", value: value)["value"]).to eq(value)
      end
    end

    it "recursively adapts arrays and maps with symbol map keys" do
      value = {
        alpha: [nil, true, 1, { beta: ["x", { gamma: 3.5 }] }],
        empty: {}
      }
      expect(neo4j_query_expect_one("RETURN $value AS value", value: value)["value"]).to eq(value)
    end

    it "transports parameters separately from query text" do
      marker = "parameter-'\"-🌍"
      row = neo4j_query_expect_one(
        "RETURN $marker AS marker, $numbers AS numbers",
        marker: marker,
        numbers: [1, 2, 3]
      )
      expect(row).to eq("marker" => marker, "numbers" => [1, 2, 3])
    end

    it "uses string keys for result rows" do
      row = neo4j_query_expect_one("RETURN 1 AS one, {nested: 2} AS map")
      expect(row.keys).to contain_exactly("one", "map")
      expect(row["map"]).to eq(nested: 2)
    end

    it "maps syntax failures narrowly and retains the upstream cause" do
      expect do
        neo4j_query("THIS IS NOT CYPHER")
      end.to raise_error(Neo4jBolt::SyntaxError) { |error| expect(error.cause).not_to be_nil }
    end

    it "streams rows through the block and returns nil" do
      returned = nil
      values = []
      expect do
        returned = neo4j_query("UNWIND range(1, 2000) AS value RETURN value") do |row|
          values << row["value"]
        end
      end.not_to raise_error
      expect(returned).to be_nil
      expect(values).to eq((1..2000).to_a)
    end
  end

  describe "graph entity adapters" do
    it "wraps nodes as Hash values with symbol property keys and legacy/additive identity" do
      node = neo4j_query_expect_one(<<~CYPHER)["node"]
        CREATE (node:Person:User {name: 'Ada', nested: ['x', 'y']})
        RETURN node
      CYPHER

      expect(node).to be_a(Neo4jBolt::Node)
      expect(node).to be_a(Hash)
      expect(node).to eq(name: "Ada", nested: ["x", "y"])
      expect(node.keys).to all(be_a(Symbol))
      expect(node.id).to be_a(Integer)
      expect(node.labels).to contain_exactly("Person", "User")
      expect(node.element_id).to be_a(String)
    end

    it "wraps relationships with properties and both legacy/additive endpoint identity" do
      relationship = neo4j_query_expect_one(<<~CYPHER)["relationship"]
        CREATE (a:Node {name: 'a'}), (b:Node {name: 'b'})
        CREATE (a)-[relationship:KNOWS {since: 2020}]->(b)
        RETURN relationship
      CYPHER

      expect(relationship).to be_a(Neo4jBolt::Relationship)
      expect(relationship).to be_a(Hash)
      expect(relationship).to eq(since: 2020)
      expect(relationship.keys).to all(be_a(Symbol))
      expect(relationship.id).to be_a(Integer)
      expect(relationship.start_node_id).to be_a(Integer)
      expect(relationship.end_node_id).to be_a(Integer)
      expect(relationship.type).to eq("KNOWS")
      expect(relationship.element_id).to be_a(String)
      expect(relationship.start_node_element_id).to be_a(String)
      expect(relationship.end_node_element_id).to be_a(String)
    end
  end

  describe "neo4j_query_expect_one" do
    it "raises for zero rows" do
      expect { neo4j_query_expect_one("UNWIND [] AS value RETURN value") }
        .to raise_error(Neo4jBolt::ExpectedOneResultError, /got 0/)
    end

    it "returns exactly one row" do
      expect(neo4j_query_expect_one("RETURN 1 AS value")).to eq("value" => 1)
    end

    it "raises for multiple rows" do
      expect { neo4j_query_expect_one("UNWIND [1, 2] AS value RETURN value") }
        .to raise_error(Neo4jBolt::ExpectedOneResultError, /got 2/)
    end
  end

  describe "transactions" do
    it "commits a clean outer transaction" do
      expect(transaction { neo4j_query("CREATE (:Marker {value: 'committed'})") }).to be_nil
      expect(marker_values).to eq(["committed"])
    end

    it "rolls back when the block raises" do
      expect do
        transaction do
          neo4j_query("CREATE (:Marker {value: 'rolled-back'})")
          raise TestRollback
        end
      end.to raise_error(TestRollback)
      expect(marker_values).to be_empty
    end

    it "uses only one actual transaction for nested compatibility transactions" do
      transaction do
        neo4j_query("CREATE (:Marker {value: 'outer-a'})")
        transaction { neo4j_query("CREATE (:Marker {value: 'inner'})") }
        neo4j_query("CREATE (:Marker {value: 'outer-b'})")
      end
      expect(marker_values).to contain_exactly("outer-a", "inner", "outer-b")
    end

    it "stays rollback-only after a rescued nested database error" do
      transaction do
        neo4j_query("CREATE (:Marker {value: 'must-rollback'})")
        begin
          transaction { neo4j_query("INVALID CYPHER") }
        rescue Neo4jBolt::SyntaxError
          # The convenient API intentionally remains rollback-only.
        end
      end
      expect(marker_values).to be_empty
    end

    it "stays rollback-only after a rescued ExpectedOneResultError" do
      transaction do
        neo4j_query("CREATE (:Marker {value: 'must-rollback'})")
        begin
          neo4j_query_expect_one("UNWIND [1, 2] AS value RETURN value")
        rescue Neo4jBolt::ExpectedOneResultError
          # The successful query still makes the compatibility transaction fail.
        end
      end
      expect(marker_values).to be_empty
    end

    it "stays rollback-only after a rescued streaming callback error" do
      transaction do
        neo4j_query("CREATE (:Marker {value: 'must-rollback'})")
        begin
          neo4j_query("UNWIND [1, 2] AS value RETURN value") { raise TestRollback }
        rescue TestRollback
          # User callback errors are transaction failures just like query errors.
        end
      end
      expect(marker_values).to be_empty
    end

    it "supports explicit rollback without requiring an upstream transaction object" do
      transaction do
        neo4j_query("CREATE (:Marker {value: 'explicit'})")
        rollback
      end
      expect(marker_values).to be_empty
    end

    it "rejects rollback outside a transaction" do
      expect { rollback }.to raise_error(Neo4jBolt::Error, /outside a transaction/)
    end
  end

  describe "constraints and indexes" do
    it "maps uniqueness violations" do
      setup_constraints_and_indexes(["User/email"], [])
      neo4j_query("CREATE (:User {email: 'same@example.test'})")
      expect do
        neo4j_query("CREATE (:User {email: 'same@example.test'})")
      end.to raise_error(Neo4jBolt::ConstraintValidationFailedError)
    end

    it "creates and cleans only neo4j_bolt_-prefixed schema entries" do
      neo4j_query("CREATE INDEX user_defined_index IF NOT EXISTS FOR (n:Other) ON (n.value)")
      setup_constraints_and_indexes(["User/email"], ["Session/expires"])

      expect(schema_names("SHOW CONSTRAINTS")).to include("neo4j_bolt_User_email")
      expect(schema_names("SHOW INDEXES")).to include("neo4j_bolt_Session_expires", "user_defined_index")

      setup_constraints_and_indexes([], [])
      expect(schema_names("SHOW CONSTRAINTS")).not_to include("neo4j_bolt_User_email")
      expect(schema_names("SHOW INDEXES")).to include("user_defined_index")
    ensure
      neo4j_query("DROP INDEX user_defined_index IF EXISTS")
    end
  end

  describe "dump and load" do
    let(:old_dump) do
      <<~DUMP
        n {"id":0,"labels":["Node"],"properties":{"marker":1,"name":"first"}}
        n {"id":1,"labels":["Node","Extra"],"properties":{"marker":2,"name":"second"}}
        r {"from":0,"to":1,"type":"BELONGS_TO","properties":{"p":2}}
      DUMP
    end

    it "preserves the textual n/r format with synthetic sequential IDs" do
      load_database_dump(StringIO.new(old_dump))
      dumped = StringIO.new
      dump_database(dumped)

      lines = dumped.string.lines
      expect(lines.grep(/^n /).size).to eq(2)
      expect(lines.grep(/^r /).size).to eq(1)
      nodes = lines.grep(/^n /).map { |line| JSON.parse(line.delete_prefix("n ")) }
      relationship = JSON.parse(lines.grep(/^r /).first.delete_prefix("r "))
      expect(nodes.map { |node| node["id"] }).to eq([0, 1])
      expect(nodes.map { |node| node["properties"]["name"] }).to contain_exactly("first", "second")
      nodes_by_id = nodes.to_h { |node| [node["id"], node] }
      expect(nodes_by_id.fetch(relationship["from"])["properties"]["name"]).to eq("first")
      expect(nodes_by_id.fetch(relationship["to"])["properties"]["name"]).to eq("second")
      expect(relationship.slice("type", "properties")).to eq(
        "type" => "BELONGS_TO", "properties" => { "p" => 2 }
      )
      expect(dumped.string).not_to match(/element_id|start_node_id|end_node_id/)
    end

    it "round-trips an old-format dump" do
      load_database_dump(StringIO.new(old_dump))
      first_dump = StringIO.new
      dump_database(first_dump)
      neo4j_query("MATCH (n) DETACH DELETE n")
      load_database_dump(StringIO.new(first_dump.string))
      second_dump = StringIO.new
      dump_database(second_dump)
      expect(normalize_dump(second_dump.string)).to eq(normalize_dump(first_dump.string))
    end

    it "refuses to append unless forced" do
      neo4j_query("CREATE (:Existing)")
      expect { load_database_dump(StringIO.new(old_dump)) }.to raise_error(Neo4jBolt::Error, /There are nodes/)
      expect { load_database_dump(StringIO.new(old_dump), force_append: true) }.not_to raise_error
    end

    it "reports progress separately from dump data and removes temporary lookup state" do
      load_progress = StringIO.new
      load_database_dump(
        StringIO.new(old_dump),
        progress_io: load_progress,
        initial_batch_size: 2
      )
      expect(load_progress.string).to include("Reading dump:", "Loading:", "Loaded:")
      expect(schema_names("SHOW INDEXES").grep(/^neo4j_bolt_load_/)).to be_empty

      dumped = StringIO.new
      dump_progress = StringIO.new
      dump_database(dumped, progress_io: dump_progress)
      expect(dump_progress.string).to include("Dumping:", "Dumped:")
      expect(dumped.string).not_to include("Dumping:", "Dumped:")
      expect(dumped.string.lines).to all(match(/\A(?:n|r) /))
    end

    it "renders colored in-place progress when pretty mode is forced" do
      previous_mode = ENV["NEO4J_BOLT_PROGRESS"]
      previous_term = ENV["TERM"]
      had_no_color = ENV.key?("NO_COLOR")
      previous_no_color = ENV["NO_COLOR"]
      ENV["NEO4J_BOLT_PROGRESS"] = "pretty"
      ENV["TERM"] = "xterm-256color"
      ENV.delete("NO_COLOR")

      progress = StringIO.new
      dump_database(StringIO.new, progress_io: progress)

      expect(progress.string).to include("\r\e[2K", "\e[36m", "\e[32m", "✓")
      expect(progress.string.count("\n")).to eq(1)
    ensure
      previous_mode.nil? ? ENV.delete("NEO4J_BOLT_PROGRESS") : ENV["NEO4J_BOLT_PROGRESS"] = previous_mode
      previous_term.nil? ? ENV.delete("TERM") : ENV["TERM"] = previous_term
      if had_no_color
        ENV["NO_COLOR"] = previous_no_color
      else
        ENV.delete("NO_COLOR")
      end
    end

    it "halves rollback-safe transaction-memory batches and keeps the reduced size" do
      upstream = StandardError.new("transaction memory")
      upstream.define_singleton_method(:code) { "Neo.ClientError.General.TransactionOutOfMemoryError" }
      upstream.define_singleton_method(:gql_status) { nil }
      memory_error = begin
        raise Neo4jBolt::Error.new("transaction memory"), cause: upstream
      rescue Neo4jBolt::Error => error
        error
      end

      attempts = []
      processed = []
      final_batch_size = send(:adaptive_each_slice, (1..8).to_a, 8, nil, "node") do |slice, _batch_size|
        attempts << slice.size
        raise memory_error if slice.size > 2

        processed.concat(slice)
      end

      expect(attempts).to eq([8, 4, 2, 2, 2, 2])
      expect(processed).to eq((1..8).to_a)
      expect(final_batch_size).to eq(2)
    end

    it "does not retry arbitrary load errors" do
      attempts = 0
      expect do
        send(:adaptive_each_slice, [1, 2], 2, nil, "node") do
          attempts += 1
          raise Neo4jBolt::Error, "not safe to retry"
        end
      end.to raise_error(Neo4jBolt::Error, /not safe to retry/)
      expect(attempts).to eq(1)
    end
  end

  describe "connection lifecycle" do
    it "waits by executing a real query" do
      expect(wait_for_neo4j).to be(true)
    end

    it "closes the pool and reconnects on a later query" do
      expect(neo4j_query_expect_one("RETURN 1 AS value")["value"]).to eq(1)
      expect(cleanup_neo4j).to be_nil
      expect(neo4j_query_expect_one("RETURN 2 AS value")["value"]).to eq(2)
    end

    it "rejects incomplete basic-auth configuration before opening a connection" do
      original_username = Neo4jBolt.bolt_username
      original_password = Neo4jBolt.bolt_password

      begin
        Neo4jBolt.bolt_username = "neo4j"
        Neo4jBolt.bolt_password = nil
        expect { neo4j_query("RETURN 1") }
          .to raise_error(Neo4jBolt::Error, /username and password must either both be set/)
      ensure
        Neo4jBolt.bolt_username = original_username
        Neo4jBolt.bolt_password = original_password
      end
    end
  end

  describe "thread safety and pooling" do
    let(:shared_client) { Object.new.extend(Neo4jBolt) }

    it "runs simultaneous parameterized queries safely through one shared object" do
      thread_count = 8
      ready = Queue.new
      start = Queue.new
      results = Queue.new

      threads = thread_count.times.map do |index|
        Thread.new do
          ready << true
          start.pop
          row = shared_client.neo4j_query_expect_one(
            "RETURN $marker AS marker, $index AS index",
            marker: "thread-#{index}", index: index
          )
          results << [row["marker"], row["index"]]
        rescue StandardError => error
          results << error
        end
      end

      thread_count.times { ready.pop }
      thread_count.times { start << true }
      threads.each(&:join)
      values = thread_count.times.map { results.pop }
      failures = values.grep(Exception)
      expect(failures).to be_empty, -> { failures.map(&:full_message).join("\n") }
      expect(values).to contain_exactly(*thread_count.times.map { |i| ["thread-#{i}", i] })
    end

    it "isolates simultaneous transactions when one rolls back and one commits" do
      ready = Queue.new
      start = Queue.new
      results = Queue.new

      rollback_thread = Thread.new do
        shared_client.transaction do
          ready << :rollback
          start.pop
          shared_client.neo4j_query("CREATE (:Marker {value: 'rolled-back-thread'})")
          shared_client.rollback
        end
        results << :rolled_back
      rescue StandardError => error
        results << error
      end

      commit_thread = Thread.new do
        shared_client.transaction do
          ready << :commit
          start.pop
          shared_client.neo4j_query("CREATE (:Marker {value: 'committed-thread'})")
        end
        results << :committed
      rescue StandardError => error
        results << error
      end

      2.times { ready.pop }
      2.times { start << true }
      [rollback_thread, commit_thread].each(&:join)
      outcomes = 2.times.map { results.pop }
      failures = outcomes.grep(Exception)
      expect(failures).to be_empty, -> { failures.map(&:full_message).join("\n") }
      expect(outcomes).to contain_exactly(:rolled_back, :committed)
      expect(marker_values).to eq(["committed-thread"])
    end
  end

  describe "CLI" do
    it "retains all documented command families" do
      stdout, stderr, status = Open3.capture3(
        RbConfig.ruby,
        File.expand_path("../bin/neo4j_bolt", __dir__),
        "--help"
      )
      expect(status).to be_success, stderr
      expect(stdout).to include("console", "clear", "dump", "load", "index", "visualize")
      expect(stdout).to include("--username", "--password", "--database")
    end
  end

  def marker_values
    neo4j_query("MATCH (n:Marker) RETURN n.value AS value ORDER BY value").map { |row| row["value"] }
  end

  def schema_names(query)
    neo4j_query(query).map { |row| row["name"] }
  end

  def normalize_dump(text)
    nodes_by_id = {}
    relationships = []
    text.each_line do |line|
      kind, json = line.split(" ", 2)
      value = JSON.parse(json)
      kind == "n" ? nodes_by_id[value["id"]] = value : relationships << value
    end

    nodes = nodes_by_id.values.map { |node| [node["labels"].sort, node["properties"]] }.sort_by(&:inspect)
    normalized_relationships = relationships.map do |relationship|
      [
        nodes_by_id.fetch(relationship["from"])["properties"],
        relationship["type"],
        relationship["properties"],
        nodes_by_id.fetch(relationship["to"])["properties"]
      ]
    end.sort_by(&:inspect)
    [nodes, normalized_relationships]
  end
end
