require 'socket'
require 'json'

class GlobalNeo4j
    include Neo4jBolt
end

$neo4j = GlobalNeo4j.new()

RSpec.describe Neo4jBolt do
    before :all do
        STDERR.puts "Launching Neo4j!"
        @thread = Thread.new do
            system("docker run --rm --name neo4j_bolt_rspec --publish-all --env NEO4J_AUTH=none neo4j:4.4-community")
        end
        loop do
            sleep 1
            begin
                inspect = JSON.parse(`docker inspect neo4j_bolt_rspec`)
                if inspect.size > 0
                    port = inspect.first['NetworkSettings']['Ports']['7687/tcp'].first['HostPort'].to_i
                    socket = TCPSocket.open('localhost', port)
                    socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
                    socket.write("\x60\x60\xB0\x17")
                    socket.write("\x00\x00\x00\x00")
                    socket.write("\x00\x00\x00\x00")
                    socket.write("\x00\x00\x00\x00")
                    socket.write("\x00\x00\x00\x00")
                    version = socket.read(4).unpack('N').first
                    STDERR.puts "Connection established!"
                    $neo4j.connect_bolt_socket('localhost', port)
                    break
                end
            rescue Errno::ECONNREFUSED, Errno::EPIPE, Errno::ECONNRESET
            end
        end
    end

    after :all do
        @thread.kill
        system("docker kill neo4j_bolt_rspec")
    end

    it 'has a version number' do
        expect(Neo4jBolt::VERSION).not_to be nil
    end

    it 'correctly transports integers up to 64 bits' do
        [-10000, -1000, -100, -10, -1, 0, 1, 10, 100, 1000, 10000,
            -9_223_372_036_854_775_808, -2_147_483_649, -2_147_483_648,
            -32769, -32768, -129, -128, -17, -16, 127, 128, 32767, 32768,
            2_147_483_647, 2_147_483_648, 9_223_372_036_854_775_807].each do |i|
                expect($neo4j.neo4j_query_expect_one("RETURN $i", {:i => i})['$i']).to eq i
        end
    end

    it 'raises an error when trying to transport integers > 64 bits' do
        [-9_223_372_036_854_775_809, 9_223_372_036_854_775_808].each do |i|
            expect do
                $neo4j.neo4j_query_expect_one("RETURN $i", {:i => i})['$i'] 
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
            expect($neo4j.neo4j_query_expect_one("RETURN $s;", {:s => s})['$s']).to eq s
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
            expect($neo4j.neo4j_query_expect_one("RETURN $s;", {:s => s})['$s']).to eq s
        end
    end

    it 'correctly yields strings to 1 MB' do
        [
            '', 'a', 'ab', 'abc',
            'abcdefghijklmnop' * 0x100,    # 4k
            'abcdefghijklmnop' * 0x1000,   # 64k
            'abcdefghijklmnop' * 0x10000,  # 1M
        ].each do |s|
            expect($neo4j.neo4j_query_expect_one("RETURN #{s.to_json} AS s;", {:s => s})['s']).to eq s
        end
    end

end