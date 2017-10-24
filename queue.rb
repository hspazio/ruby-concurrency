require 'thread'

work = Queue.new

producer = Thread.new do
  10.times.each do |i|
    work << "message #{i}"
  end

  work << :done # try commenting this out. The runtime triggers a deadlock because
  # the consumer will wait forever
end

consumer = Thread.new do
  loop do
    message = work.pop
    break if message == :done
    puts "Received: #{message}"
    sleep 1 # simulate some work to do
  end
end

puts "start"
producer.join
consumer.join
puts "end"
