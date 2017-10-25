require 'thread'

class Publisher
  def initialize
    @subscriptions = { update: [], create: [] }
  end

  def subscribe(subscriber, events)
    events.each do |event|
      @subscriptions[event] << subscriber
    end
  end

  def publish(message)
    event = message[:event]
    @subscriptions[event].each do |subscriber|
      subscriber << message
    end
  end
end

class Worker
  attr_reader :name

  def initialize(name)
    @name = name
    @queue = Queue.new
    @perform_async = Thread.new { perform }
  end

  def <<(job)
    @queue << job
  end

  def run
    @perform_async.join
  end

  private

  def perform
    while job = @queue.pop do
      break if job == :done
      puts "Worker #{name} received job: #{job}"
      sleep rand(5) # simulating performing job
    end
  end
end

w1 = Worker.new 1
w2 = Worker.new 2
w3 = Worker.new 3

pub = Publisher.new
pub.subscribe(w1, [:update])
pub.subscribe(w2, [:create])
pub.subscribe(w3, [:create, :update])

data_stream = [
  { event: :create, data: "1. creating widget" },
  { event: :update, data: "2. updating widget" },
  { event: :create, data: "3. creating widget" },
  { event: :create, data: "4. creating widget" },
  { event: :update, data: "5. updating widget" },
  { event: :update, data: "6. updating widget" },
  { event: :create, data: "7. creating widget" },
  { event: :update, data: "8. updating widget" }
]

events_producer = Thread.new do 
  data_stream.each do |message|
    puts "publishing #{message}"
    pub.publish(message)
  end
end

events_producer.join
w1.run
w2.run
w3.run
pub.run
