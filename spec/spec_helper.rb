# frozen_string_literal: true

require "bundler/setup"
require "neo4j_bolt"

test_port = ENV["NEO4J_BOLT_TEST_PORT"]
abort "NEO4J_BOLT_TEST_PORT must point at a disposable test database" unless test_port

Neo4jBolt.bolt_host = ENV.fetch("NEO4J_BOLT_TEST_HOST", "localhost")
Neo4jBolt.bolt_port = Integer(test_port)
Neo4jBolt.bolt_verbosity = Integer(ENV.fetch("NEO4J_BOLT_TEST_VERBOSITY", "0"))

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_status"
  config.disable_monkey_patching!
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
  config.order = :random

  config.before(:suite) do
    Neo4jBolt.wait_for_neo4j
  end

  config.before do
    Neo4jBolt.setup_constraints_and_indexes([], [])
    Neo4jBolt.neo4j_query("MATCH (n) DETACH DELETE n")
  end

  config.after(:suite) do
    Neo4jBolt.setup_constraints_and_indexes([], [])
    Neo4jBolt.cleanup_neo4j
  rescue Neo4jBolt::Error
    # Preserve the actual spec failure if the disposable server already exited.
  end
end
