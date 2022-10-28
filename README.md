# Neo4jBolt

A Neo4j/Bolt driver written in pure Ruby. Currently only supporting Neo4j 4.4.

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

### Connecting to a Neo4j database

Specify your Bolt host and port (if you omit this it will be localhost:7687 by default):

```ruby
Neo4jBolt.bolt_host = 'localhost'
Neo4jBolt.bolt_port = 7687
```

Use `cleanup_neo4j` to disconnect (this is important when running a web app – it might be a good idea to close a socket once we're done with it so we don't run out of ports).


### Running queries

Use `neo4j_query` to run a query and receive all results:

```ruby
entries = neo4j_query("MATCH (n) RETURN n;")
```
Alternatively, specify a block to make use of Neo4j's streaming capabilities and receive entries one by one:

```ruby
neo4j_query("MATCH (n) RETURN n;") do |entry|
    # handle entry here
end
```

Using streaming avoids memory hog since it prevents having to read all entries into memory before handling them. Nodes are returned as `Neo4jBolt::Node`, relationships as `Neo4jBolt::Relationship`. Both are subclasses of `Hash`, providing access to all properties plus a few extra details:

- `Neo4jBolt::Node`: `id`, `labels`
- `Neo4jBolt::Relationship`: `id`, `start_node_id`, `end_node_id`, `type`

```ruby
node = neo4j_query_expect_one("CREATE (n:Node {a: 1, b: 2}) RETURN n;")['n']
# All nodes returned from Neo4j are a Neo4jBolt::Node
# It's a subclass of Hash and it stores all the node's
# properties plus two attributes called id and labels:
puts node.id
puts node.labels
puts node.keys
node.each_pair { |k, v| puts "#{k}: #{v}" }
puts node.to_json
```
Use `neo4j_query_expect_one` if you want to make sure there's exactly one entry to be returned:

```ruby
node = neo4j_query_expect_one("MATCH (n) RETURN n LIMIT 1;")['n']
```

If there's zero, two, or more results, this will raise a `ExpectedOneResultError`.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/specht/neo4j_bolt.

