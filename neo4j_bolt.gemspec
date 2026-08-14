require_relative 'lib/neo4j_bolt/version'

Gem::Specification.new do |spec|
  spec.name          = "neo4j_bolt"
  spec.version       = Neo4jBolt::VERSION
  spec.authors       = ["Michael Specht"]
  spec.email         = ["micha.specht@gmail.com"]
  spec.licenses      = ['GPL-3.0-only']

  spec.summary       = "A compatibility and convenience layer backed by neo4j-ruby-driver"
  spec.homepage      = "https://github.com/specht/neo4j_bolt"
  spec.required_ruby_version = Gem::Requirement.new(">= 3.4")

  # spec.metadata["allowed_push_host"] = "https://github.com/specht/neo4j_bolt"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/specht/neo4j_bolt"
  spec.metadata["changelog_uri"] = "https://github.com/specht/neo4j_bolt"

  spec.files         = Dir[
    "LICENSE",
    "README.md",
    "lib/**/*.rb",
    "bin/console",
    "bin/neo4j_bolt"
  ]
  spec.bindir        = "bin"
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "rspec", "~> 3.13"
  spec.add_runtime_dependency "neo4j-ruby-driver", "= 6.2.1.beta.4"
  spec.add_runtime_dependency "gli", "~> 2.22"
  spec.add_runtime_dependency "pry", "~> 0.15"
end
