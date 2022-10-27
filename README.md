# Neo4jBolt

A Neo4j/Bolt driver written in pure Ruby.

🚀 **Streaming support!** Neo4j sends responses in chunks of max. 64 kb. When running queries, specify a block to obtain rows as soon as they arrive.

## Supported and unsupported aspects

|  | Supported | Unsupported |
|-|-|-
| Neo4j | 4.4 | 1.x 2.x 3.x <br /> 4.0 4.1 4.2 4.3 <br /> 5.x |
| Bolt | 4.4 | 1 2 3 4.0 4.1 4.2 4.3 |


## Installation

Add this line to your application's Gemfile:

```ruby
gem 'neo4j_bolt'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install neo4j_bolt

## Usage

In order to use this gem, you need a running Neo4j database. You can start one using the following command:

```
docker run --rm --env NEO4J_AUTH=none --publish 7687:7687 neo4j:4.4-community
```

TODO: Write usage instructions here

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/specht/neo4j_bolt.

