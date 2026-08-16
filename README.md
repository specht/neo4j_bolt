# Neo4jBolt

Neo4jBolt 0.4 is a small compatibility and convenience layer for Ruby applications, backed by [neo4j-ruby-driver](https://github.com/neo4jrb/neo4j-ruby-driver). It preserves the straightforward `Neo4jBolt` application API while delegating connections, pooling, Bolt negotiation, protocol state, PackStream, reconnects, and modern Neo4j value support to the upstream driver.

Neo4jBolt no longer implements the Bolt wire protocol itself.

`0.4.4` requires Ruby 3.4 or newer and pins `neo4j-ruby-driver` to `6.2.1.beta.4`. Applications on older Rubies can remain on the Neo4jBolt 0.3.x line; this prerelease is intentionally not an automatic upgrade for them.

## Installation

For this prerelease, specify the version explicitly:

```ruby
gem "neo4j_bolt", "0.4.4"
```

Then run `bundle install`. A running Neo4j database is required.

## Connecting

The existing host, port, and verbosity settings remain available:

```ruby
Neo4jBolt.bolt_host = "localhost"
Neo4jBolt.bolt_port = 7687
Neo4jBolt.bolt_verbosity = 0
```

Authentication and database selection can use the standard Neo4j client environment variables:

```bash
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=secret
export NEO4J_DATABASE=neo4j
```

They are read when `neo4j_bolt` is loaded. Explicit Ruby configuration overrides those defaults:

```ruby
Neo4jBolt.bolt_username = "neo4j"
Neo4jBolt.bolt_password = "secret"
Neo4jBolt.bolt_database = "movies"
```

If both username and password are unset, Neo4jBolt keeps the existing unauthenticated connection behavior. If only one is set, the first query raises `Neo4jBolt::Error`. Leaving the database unset lets Neo4j choose the user's/server's default database.

Applications can include the module as before:

```ruby
include Neo4jBolt
```

They can also extend another object or call the convenience methods directly on `Neo4jBolt`.

One lazily initialized upstream driver owns a thread-safe connection pool. Each standalone query uses a short-lived session and returns its connection to that pool after its result has been consumed. Queries from several Puma threads are not globally serialized.

`cleanup_neo4j` safely retires the driver and its pool after active operations finish. A later query creates a new driver. Changing host, port, username, or password also retires the pool so later connections use the new settings. Database selection is a session setting and therefore applies to newly opened sessions without rebuilding the driver. Do not call cleanup from inside an active query or transaction on the same thread.

## Running queries

Materialize all result rows:

```ruby
rows = neo4j_query("MATCH (person:Person) RETURN person.name AS name")
puts rows.first["name"]
```

Or process records incrementally without first materializing the complete result:

```ruby
neo4j_query("MATCH (person:Person) RETURN person") do |row|
  puts row["person"][:name]
end
```

The block form returns `nil`. Its upstream session, result, and pooled connection remain alive for the duration of iteration.

Parameters stay separate from Cypher:

```ruby
row = neo4j_query_expect_one(
  "MATCH (person:Person {email: $email}) RETURN person",
  email: "ada@example.test"
)
```

`neo4j_query_expect_one` raises `Neo4jBolt::ExpectedOneResultError` unless the query produces exactly one row.

### Result compatibility

Result-row keys are strings:

```ruby
row["person"]
```

Nodes and relationships remain Hash subclasses whose property keys are symbols:

```ruby
node = row["person"]
node[:name]
node.id
node.labels
node.element_id
```

`Neo4jBolt::Node` preserves `id` and `labels` and additively exposes `element_id`.

`Neo4jBolt::Relationship` preserves `id`, `start_node_id`, `end_node_id`, and `type`, and additively exposes `element_id`, `start_node_element_id`, and `end_node_element_id`.

Arrays and maps are adapted recursively. Map/property keys inside values are symbols, matching Neo4jBolt 0.3 behavior. Modern upstream values that 0.3 could not represent—such as temporal, spatial, path, byte, duration, and UUID values—pass through as upstream value objects. This is additive support.

Signed 64-bit integer limits are checked before transport. Out-of-range integers raise `Neo4jBolt::IntegerOutOfRangeError`.

## Transactions

The convenient compatibility API is unchanged:

```ruby
transaction do
  neo4j_query("CREATE (:Person {name: 'Ada'})")
  neo4j_query("CREATE (:Person {name: 'Grace'})")
end
```

Nested `transaction` blocks reuse one actual upstream session and transaction. Applications do not receive or pass an upstream transaction object.

Transaction context is isolated by calling thread and by the object using `Neo4jBolt`. Two threads using the same object receive independent upstream transactions and pooled connections.

Transactions are rollback-only after any nested operation raises, even if application code rescues the exception later. This includes database failures and `ExpectedOneResultError`:

```ruby
transaction do
  neo4j_query("CREATE (:Marker)")

  begin
    neo4j_query("invalid cypher")
  rescue Neo4jBolt::SyntaxError
  end
end
# Nothing is committed.
```

Calling `rollback` inside a transaction marks that outer transaction rollback-only and returns `nil`. Calling it outside a transaction raises `Neo4jBolt::Error`; the old 0.3 implementation exposed the method but accidentally delegated to a nonexistent private implementation.

Upstream server/driver failures are translated to `Neo4jBolt::Error`. Syntax and uniqueness-constraint failures are translated narrowly to `Neo4jBolt::SyntaxError` and `Neo4jBolt::ConstraintValidationFailedError`. The upstream exception remains available as `error.cause`.

## Constraints and indexes

The existing setup format remains supported:

```ruby
setup_constraints_and_indexes(
  ["User/email", "Session/sid"],
  ["Session/expires"]
)
```

Managed entries retain the `neo4j_bolt_` prefix. Setup removes obsolete entries with that prefix only; it does not remove arbitrary application-defined indexes or constraints.

## Dump and load

The textual format is unchanged:

```text
n {"id":0,"labels":["Person"],"properties":{"name":"Ada"}}
n {"id":1,"labels":["Person"],"properties":{"name":"Grace"}}
r {"from":0,"to":1,"type":"KNOWS","properties":{"since":2024}}
```

```ruby
File.open("database.dump", "w") { |io| dump_database(io, progress_io: $stderr) }
File.open("database.dump", "r") do |io|
  load_database_dump(io, progress_io: $stderr)
end
```

Passing `progress_io:` is optional for the Ruby API. The CLI always reports dump/load progress on stderr, so dump data written to stdout remains safe to redirect or pipe. In `auto` mode a terminal gets a colored, in-place progress bar while redirected stderr receives periodic plain-text progress lines. Set `NEO4J_BOLT_PROGRESS=pretty` to force the terminal display through a container or other wrapper that hides the TTY, or `NEO4J_BOLT_PROGRESS=plain` to force log-friendly output. `NO_COLOR` disables colors without disabling the in-place display.

Both `dump` and `load` accept `--color COLOR` to choose the spinner and filled progress-bar accent. The default is `cyan`. The Ruby API exposes the same setting as `progress_color:`. Available colors are `red`, `green`, `yellow`, `blue`, `magenta`, `cyan`, `white`, `bright-red`, `bright-green`, `bright-yellow`, `bright-blue`, `bright-magenta`, `bright-cyan`, and `bright-white`.

```bash
neo4j_bolt dump --color magenta -o database.dump
neo4j_bolt load --color bright-blue database.dump
```

The `0, 1, 2, ...` IDs are synthetic dump-local IDs. Database-internal IDs and element IDs are never written to the persistent format. Dumping keeps the count, node, and relationship reads in one transaction, so the driver-provided entity identity used to connect the two streamed result sets stays within Neo4j's transaction-scoped identity guarantee. To keep dump numbering deterministic, ordering uses `elementId()` on modern Neo4j and an internal `id()` fallback only on Neo4j 4.4, where `elementId()` does not exist. Old Neo4jBolt dumps remain loadable.

For relational loads, the adapter assigns a random temporary label and dump-ID property to imported nodes. After the nodes are committed it builds a temporary `neo4j_bolt_` index on that property, uses indexed lookups while creating relationships, drops the index, and removes the temporary metadata in batches. No database-internal identity is carried from one load transaction to another. Loading a relational dump therefore requires permission to create and drop an index.

Loads start with batches of 5,000 records by default. This is an initial ceiling rather than a claimed optimum: if Neo4j returns a transaction-memory/resource error whose server semantics guarantee rollback, the loader halves the failed batch and retries it, then keeps the smaller size for the rest of that phase. Node and relationship phases adapt independently. Other errors are not retried, because a generic connection failure cannot safely prove that a `CREATE` transaction did not commit. Callers can change the initial ceiling with `initial_batch_size:`; the CLI exposes the same setting as `--batch-size`.

Loading requires an empty database unless `force_append: true` is passed. `load_database_dump` owns its batch transactions and therefore rejects being called from inside `transaction`.

## CLI

The `neo4j_bolt` executable retains these commands:

| Command | Purpose |
| --- | --- |
| `neo4j_bolt console` | Open an IRB console with Neo4jBolt loaded |
| `neo4j_bolt clear --srsly` | Delete all nodes and relationships |
| `neo4j_bolt dump [--color COLOR]` | Write the textual database dump |
| `neo4j_bolt load [--force] [--batch-size N] [--color COLOR] PATH` | Load a textual dump |
| `neo4j_bolt index ls` | List constraints and indexes |
| `neo4j_bolt index rm --force` | Remove all constraints and indexes |
| `neo4j_bolt visualize` | Generate a GraphViz document |

Use `--host HOST:PORT`, `--username USER`, `--password PASSWORD`, and `--database DATABASE` to override connection settings. Username, password, and database otherwise inherit `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE`. Prefer the password environment variable for routine use because command-line arguments may be visible in shell history or process listings. `gli` remains a runtime dependency for the CLI, and `pry` remains for the separate `bin/console` executable.

## Tested Neo4j versions

The same complete integration suite is run against these exact Community images:

- `neo4j:4.4.48-community`
- `neo4j:5.26.28-community`
- `neo4j:2026.06.0-community`

No compatibility beyond this matrix is claimed for `0.4.4`.

Run one modern LTS target:

```bash
bundle exec rake spec
```

Run the complete sequential matrix:

```bash
bundle exec rake spec:matrix
```

The harness creates uniquely named disposable containers with Basic authentication enabled, dynamically publishes Bolt ports, waits by establishing a real driver/query connection, and cleans every container through shell traps. A developer does not need to start Neo4j manually.

To point RSpec itself at an already disposable database, set `NEO4J_BOLT_TEST_HOST` and `NEO4J_BOLT_TEST_PORT`. If it requires authentication or an explicit database, also set `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE`. The explicit port requirement protects real databases from the destructive integration suite.

## Compatibility inventory for 0.4

| API | Status |
| --- | --- |
| `bolt_host`, `bolt_port`, `bolt_verbosity` | Preserved |
| `bolt_username`, `bolt_password`, `bolt_database` | Additive authentication/database configuration |
| Included/extended/module-style use | Preserved |
| `neo4j_query`, including incremental block form | Preserved via adapter |
| `neo4j_query_expect_one` | Preserved via adapter |
| `transaction`, nesting, rollback-only behavior | Preserved via thread-local adapter |
| `rollback` | Preserved and repaired as rollback-only |
| `cleanup_neo4j`, `wait_for_neo4j` | Preserved via pooled driver lifecycle |
| `setup_constraints_and_indexes` | Preserved with current Cypher |
| `dump_database`, `load_database_dump` | Preserved; relationship reconstruction modernized |
| Named error classes | Preserved with narrow upstream translation |
| `Node` and `Relationship` Hash behavior | Preserved via recursive value adapter |
| Element identity and modern values | Additive behavior |
| CLI commands | Preserved |
| `BoltSocket`, `BoltBuffer`, protocol markers/state/parser/packer | Intentionally removed private implementation details |

## 0.4.4 migration notes

- Ruby 3.4 or newer is required; Ruby 2.x/3.0–3.3 applications should stay on 0.3.x until upgraded.
- The exact prerelease upstream dependency is pinned while no stable `neo4j-ruby-driver` 6.2.x exists.
- Connections are pooled and safe for concurrent use instead of one mutable socket per including object.
- Basic authentication and database selection are now exposed through `bolt_username`, `bolt_password`, and `bolt_database`, with `NEO4J_USERNAME`, `NEO4J_PASSWORD`, and `NEO4J_DATABASE` as environment defaults.
- TLS, routing, URI schemes, and other upstream-driver configuration are still not exposed through the compatibility API.
- `BoltSocket`, `BoltBuffer`, `ServerState`, `BoltMarker`, `UnexpectedServerResponse`, `State`, and `CypherError` were undocumented wire internals and are removed.

## Development

Run `bin/setup`, then `bundle exec rake spec` or `bundle exec rake spec:matrix`. Build without publishing with `bundle exec rake build`.

Bug reports and pull requests are welcome at <https://github.com/specht/neo4j_bolt>.

