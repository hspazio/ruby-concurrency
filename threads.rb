require 'thread'

puts 'start'

threads = []
threads << Thread.new do
  loop do
    puts 'A'
    sleep rand(5) + 1
  end
end
  
threads << Thread.new do
  loop do
    puts 'B'
    sleep rand(1) + 1
  end
end

threads.map(&:join)

puts 'end'
