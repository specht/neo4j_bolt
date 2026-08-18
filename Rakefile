# frozen_string_literal: true

require "bundler/gem_tasks"

desc "Run the compatibility suite against Neo4j 5.26.28 Community"
task :spec do
  if ENV["NEO4J_BOLT_TEST_PORT"]
    sh "bundle exec rspec"
  else
    sh "./script/test-matrix --only neo4j:5.26.28-community"
  end
end

namespace :spec do
  desc "Run the complete pinned Neo4j Community/Enterprise compatibility matrix"
  task :matrix do
    sh "./script/test-matrix"
  end
end

task default: :spec
