require 'thread'

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

class Scheduler
  def initialize(workers)
    @workers = workers
  end

  def round_robin(jobs)
    jobs.each_with_index do |job, index|
      worker = @workers[index % @workers.size]
      worker << job
    end
  end

  def shutdown
    @workers.each { |w| w << :done }
  end
end

NUM_WORKERS = 5
workers = Array.new(NUM_WORKERS) { |i| Worker.new(i) }
scheduler = Scheduler.new(workers)

producer = Thread.new do
  jobs = Array.new(20) { |n| "message #{n}" }
  scheduler.round_robin(jobs)
  scheduler.shutdown
end

workers.map(&:run)
producer.join

