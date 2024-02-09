require 'socket'
require 'json'

# Be careful to specify a database here, this script will
# clear all contents. If you don't know what you're doing,
# set GOT_NEO4J to nil to launch a Neo4j database via
# Docker for the sole purpose of testing
# GOT_NEO4J = ENV['QTS_DEVELOPMENT'] == '1' ? ['localhost', 7687] : nil
GOT_NEO4J = nil

RSpec.describe Neo4jBolt do
    include Neo4jBolt

    class TempError < StandardError; end;

    before :all do
        if GOT_NEO4J
            Neo4jBolt.bolt_host = GOT_NEO4J[0]
            Neo4jBolt.bolt_port = GOT_NEO4J[1]
        else
            system("docker run --detach --rm --name neo4j_bolt_rspec --publish-all --env NEO4J_AUTH=none neo4j:4.4-community")
            loop do
                sleep 1
                begin
                    inspect = JSON.parse(`docker inspect neo4j_bolt_rspec`)
                    if inspect.size > 0
                        port = inspect.first['NetworkSettings']['Ports']['7687/tcp'].first['HostPort'].to_i
                        socket = TCPSocket.open('localhost', port)
                        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
                        socket.write("\x60\x60\xB0\x17")
                        socket.write("\x00\x00\x04\x04")
                        socket.write("\x00\x00\x00\x00")
                        socket.write("\x00\x00\x00\x00")
                        socket.write("\x00\x00\x00\x00")
                        version = socket.read(4).unpack('N').first
                        Neo4jBolt.bolt_host = 'localhost'
                        Neo4jBolt.bolt_port = port
                        break
                    end
                rescue Errno::ECONNREFUSED, Errno::EPIPE, Errno::ECONNRESET
                end
            end
        end
    end

    after :all do
        unless GOT_NEO4J
            # @thread.kill
            system("docker kill neo4j_bolt_rspec")
        end
    end

    it 'has a version number' do
        expect(Neo4jBolt::VERSION).not_to be nil
    end

    it 'correctly transports integers up to 64 bits' do
        [-10000, -1000, -100, -10, -1, 0, 1, 10, 100, 1000, 10000,
            -9_223_372_036_854_775_808, -2_147_483_649, -2_147_483_648,
            -32769, -32768, -129, -128, -17, -16, 127, 128, 32767, 32768,
            2_147_483_647, 2_147_483_648, 9_223_372_036_854_775_807].each do |i|
                expect(neo4j_query_expect_one("RETURN $i", {:i => i})['$i']).to eq i
        end
    end

    it 'raises an error when trying to transport integers > 64 bits' do
        [-9_223_372_036_854_775_809, 9_223_372_036_854_775_808].each do |i|
            expect do
                neo4j_query_expect_one("RETURN $i", {:i => i})['$i']
            end.to raise_error(Neo4jBolt::Error)
        end
    end

    it 'correctly transports strings to 1 MB' do
        [
            '', 'a', 'ab', 'abc',
            'abcdefghijklmnop' * 0x100,    # 4k
            'abcdefghijklmnop' * 0x1000,   # 64k
            'abcdefghijklmnop' * 0x10000,  # 1M
        ].each do |s|
            expect(neo4j_query_expect_one("RETURN $s;", {:s => s})['$s']).to eq s
        end
    end

    it 'correctly transports UTF-8 encoded Unicode strings' do
        [
            '🌩️',
            '∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i), ∀x∈ℝ: ⌈x⌉ = −⌊−x⌋, α ∧ ¬β = ¬(¬α ∨ β)',
            'ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ, ⊥ < a ≠ b ≡ c ≤ d ≪ ⊤ ⇒ (A ⇔ B)',
            '2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm',
            'ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn',
            'Y [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]',
            'Δημοσθένους, Γ´ ᾿Ολυνθιακὸς',
            'გთხოვთ ახლავე გაიაროთ რეგისტრაცია Unicode-ის მეათე საერთაშორისო კონფერენციაზე დასასწრებად, რომელიც გაიმართება 10-12 მარტს, ქ. მაინცში, გერმანიაში. კონფერენცია შეჰკრებს ერთად მსოფლიოს ექსპერტებს ისეთ დარგებში როგორიცაა ინტერნეტი და Unicode-ი, ინტერნაციონალიზაცია და ლოკალიზაცია, Unicode-ის გამოყენებაოპერაციულ სისტემებსა, და გამოყენებით პროგრამებში, შრიფტებში, ტექსტების დამუშავებასა და მრავალენოვან კომპიუტერულ სისტემებში.',
            'Зарегистрируйтесь сейчас на Десятую Международную Конференцию по Unicode, которая состоится 10-12 марта 1997 года в Майнце в Германии. Конференция соберет широкий круг экспертов по  вопросам глобального Интернета и Unicode, локализации и интернационализации, воплощению и применению Unicode в различных операционных системах и программных приложениях, шрифтах, верстке и многоязычных компьютерных системах.',
            'ABCDEFGHIJKLMNOPQRSTUVWXYZ /0123456789abcdefghijklmnopqrstuvwxyz £©µÀÆÖÞßéöÿ–—‘“”„†•…‰™œŠŸž€ ΑΒΓΔΩαβγδω АБВГДабвгд∀∂∈ℝ∧∪≡∞ ↑↗↨↻⇣ ┐┼╔╘░►☺♀ ﬁ�⑀₂ἠḂӥẄɐː⍎אԱა'
        ].each do |s|
            expect(neo4j_query_expect_one("RETURN $s;", {:s => s})['$s']).to eq s
        end
    end

    it 'correctly transports 64 bit floats' do
        [
            1.0, Math::PI, Float::INFINITY, -Float::INFINITY,
            Math::sqrt(2.0)
        ].each do |f|
            expect(neo4j_query_expect_one("RETURN $f;", {:f => f})['$f']).to eq f
        end
    end

    it 'correctly transports dictionaries' do
        [
            {},
            {a: {b: {}}},
            {a: {b: {c: {d: {}}}}},
            {hey: 1, yes: 2, no: 'of course'},
            {chars: ['A', 'B', 'C', {well: 'no', then: {}}], digits: [1, 2, 3], nothing: nil},
        ].each do |x|
            expect(neo4j_query_expect_one("RETURN $x;", {:x => x})['$x']).to eq x
        end
    end

    it 'correctly transports arrays' do
        [
            [],
            [1],
            [nil],
            [1, 2, 3, 4, 5, 6],
            [[[[[[[]]]]]]],
            [[],[],[],[]],
            [1, [2, [3, [4, [5, [6]]]]]]
        ].each do |x|
            expect(neo4j_query_expect_one("RETURN $x;", {:x => x})['$x']).to eq x
        end
    end

    it 'correctly runs a correct query' do
        expect do
            neo4j_query_expect_one("MATCH (n) RETURN COUNT(n) AS n;")
        end.to_not raise_error
    end

    it 'raises error on Neo4jBolt::SyntaxError' do
        expect do
            transaction do
                neo4j_query_expect_one("SMUDGE (n) RETURN COUNT(n) AS n;")
            end
        end.to raise_error Neo4jBolt::SyntaxError
    end

    it 'correctly yields strings to 1 MB' do
        [
            '', 'a', 'ab', 'abc',
            'abcdefghijklmnop' * 0x100,    # 4k
            'abcdefghijklmnop' * 0x1000,   # 64k
            'abcdefghijklmnop' * 0x10000,  # 1M
        ].each do |s|
            expect(neo4j_query_expect_one("RETURN #{s.to_json} AS s;", {:s => s})['s']).to eq s
        end
    end

    it 'has atomic transactions after Neo4jBolt::SyntaxError' do
        transaction do
            neo4j_query("CREATE (n:Node) SET n.marker = 1;")
            begin
                neo4j_query("SHEESH")
            rescue Neo4jBolt::SyntaxError
            end
        end
        dump = StringIO.open do |io|
            dump_database(io)
            io.string
        end
        expect(dump.size).to eq 0
    end

    it 'has atomic transactions after Neo4jBolt::ExpectedOneResultError' do
        begin
            transaction do
                neo4j_query("CREATE (n:Node) SET n.marker = 1;")
                neo4j_query("CREATE (n:Node) SET n.marker = 1;")
                neo4j_query_expect_one("MATCH (n) RETURN n;")
            end
        rescue Neo4jBolt::ExpectedOneResultError
        end
        dump = StringIO.open do |io|
            dump_database(io)
            io.string
        end
        expect(dump.size).to eq 0
    end

    it 'can create database dumps' do
        begin
            transaction do
                neo4j_query("CREATE (n:Node) SET n.marker = 1;")
                neo4j_query("CREATE (n:Node) SET n.marker = 2;")
                neo4j_query("MATCH (a:Node {marker: 1}), (b:Node {marker: 2}) CREATE (a)-[:BELONGS_TO {p: 2}]->(b);")
                dump = StringIO.open do |io|
                    dump_database(io)
                    io.string
                end
                expect(dump).to eq <<~END_OF_STRING
                    n {"id":0,"labels":["Node"],"properties":{"marker":1}}
                    n {"id":1,"labels":["Node"],"properties":{"marker":2}}
                    r {"from":0,"to":1,"type":"BELONGS_TO","properties":{"p":2}}
                END_OF_STRING
                # raise error to roll back transaction
                raise TempError.new
            end
        rescue TempError
        end
    end

    it 'can load database dumps' do
        dump = nil
        begin
            transaction do
                neo4j_query("CREATE (n:Node) SET n.marker = 1;")
                neo4j_query("CREATE (n:Node) SET n.marker = 2;")
                neo4j_query("MATCH (a:Node {marker: 1}), (b:Node {marker: 2}) CREATE (a)-[:BELONGS_TO {p: 2}]->(b);")
                dump = StringIO.open do |io|
                    dump_database(io)
                    io.string
                end
                raise TempError.new
            end
        rescue TempError
        end
        begin
            buffer = StringIO.new(dump)
            load_database_dump(buffer)
            new_dump = StringIO.open do |io|
                dump_database(io)
                io.string
            end
            expect(new_dump).to eq <<~END_OF_STRING
                n {"id":0,"labels":["Node"],"properties":{"marker":1}}
                n {"id":1,"labels":["Node"],"properties":{"marker":2}}
                r {"from":0,"to":1,"type":"BELONGS_TO","properties":{"p":2}}
            END_OF_STRING
            raise TempError.new
        rescue TempError
        end
    end

    it 'can return nodes (streaming with block)' do
        begin
            transaction do
                neo4j_query("CREATE (n:Node) SET n.marker = 1;")
                neo4j_query("CREATE (n:Node) SET n.marker = 2;")
                seen_markers = Set.new()
                neo4j_query("MATCH (n:Node) RETURN n ORDER BY n.marker;") do |row|
                    node = row['n']
                    expect(node).to be_a Neo4jBolt::Node
                    expect(node.id).to be_a Integer
                    expect(node.labels).to be_a Array
                    expect(node.labels).to eq ['Node']
                    expect(node).to be_a Hash
                    expect(node[:marker]).to be_a Integer
                    seen_markers << node[:marker]
                end
                expect(seen_markers.include?(1)).to be true
                expect(seen_markers.include?(2)).to be true
                # raise error to roll back transaction
                raise TempError.new
            end
        rescue TempError
        end
    end

    it 'can return nodes (without streaming)' do
        begin
            transaction do
                neo4j_query("CREATE (n:Node) SET n.marker = 1;")
                neo4j_query("CREATE (n:Node) SET n.marker = 2;")
                seen_markers = Set.new()
                neo4j_query("MATCH (n:Node) RETURN n ORDER BY n.marker;").each do |row|
                    node = row['n']
                    expect(node).to be_a Neo4jBolt::Node
                    expect(node.id).to be_a Integer
                    expect(node.labels).to be_a Array
                    expect(node.labels).to eq ['Node']
                    expect(node).to be_a Hash
                    expect(node[:marker]).to be_a Integer
                    seen_markers << node[:marker]
                end
                expect(seen_markers.include?(1)).to be true
                expect(seen_markers.include?(2)).to be true
                # raise error to roll back transaction
                raise TempError.new
            end
        rescue TempError
        end
    end

    it 'can close the connection' do
        expect do
            neo4j_query("MATCH (n) RETURN n;")
            cleanup_neo4j
        end.not_to raise_error
    end

    it 'honors uniqueness constraints' do
        setup_constraints_and_indexes(["Node/marker"], [])
        # neo4j_query("CREATE CONSTRAINT Node_marker IF NOT EXISTS FOR (n:Node) REQUIRE n.marker IS UNIQUE")
        expect do
            transaction do
                neo4j_query_expect_one("CREATE (n:Node {marker: 1}) RETURN n;")
                neo4j_query_expect_one("CREATE (n:Node {marker: 1}) RETURN n;")
            end
        end.to raise_error(Neo4jBolt::ConstraintValidationFailedError)
        setup_constraints_and_indexes([], [])
    end

    it 'can handle concurrect read requests' do
        expect do
            transaction do
                (0...10000).each do |i|
                    neo4j_query("CREATE (i:Integer {value: $i, count: 0});", {:i => i});
                end
            end
            ts0 = Time.now.to_f
            (0...10).each do |f|
                fork do
                    node_count = neo4j_query("MATCH (i:Integer) RETURN i;").map { |x| x['i'] }.size
                    unless node_count >= 1000
                        raise 'Wrong count!'
                    end
                end
            end
            Process.waitall
            ts1 = Time.now.to_f
            STDERR.puts "This took #{sprintf('%1.2f', ts1 - ts0)} s."
        end.not_to raise_error
    end

    it 'can handle concurrect write requests' do
        expect do
            ts0 = Time.now.to_f
            (0...10).each do |f|
                fork do
                    transaction do
                        (0...500).each do |i|
                            count = neo4j_query_expect_one("MATCH (i:Integer {value: $i}) SET i.count = i.count + 1 RETURN i;", {:i => i})['i'][:count]
                        end
                    end
                end
            end
            Process.waitall
            (0...500).each do |i|
                count = neo4j_query_expect_one("MATCH (i:Integer {value: $i}) RETURN i;", {:i => i})['i'][:count]
                STDERR.print "#{count} "
            end
            STDERR.puts
            ts1 = Time.now.to_f
            STDERR.puts "This took #{sprintf('%1.2f', ts1 - ts0)} s."
        end.not_to raise_error
    end
end