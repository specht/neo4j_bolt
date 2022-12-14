require_relative 'lib/neo4j_bolt/version'

Gem::Specification.new do |spec|
  spec.name          = "neo4j_bolt"
  spec.version       = Neo4jBolt::VERSION
  spec.authors       = ["Michael Specht"]
  spec.email         = ["micha.specht@gmail.com"]
  spec.licenses      = ['GPL-3.0-only']

  spec.summary       = "A Neo4j/Bolt driver written in pure Ruby"
  spec.homepage      = "https://github.com/specht/neo4j_bolt"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")

  # spec.metadata["allowed_push_host"] = "https://github.com/specht/neo4j_bolt"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/specht/neo4j_bolt"
  spec.metadata["changelog_uri"] = "https://github.com/specht/neo4j_bolt"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "bin"
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rspec", "~> 3.2"
  spec.add_runtime_dependency "gli"
  spec.add_runtime_dependency "pry"
end
