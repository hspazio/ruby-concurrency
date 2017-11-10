require 'minitest/autorun'

class Worker
  attr_reader :name

  def initialize(name)
    @name = name
    @queue = Queue.new
  end

  def jobs_count
    @queue.size
  end

  def <<(job)
    @queue << job
  end

  def run
    Thread.new { perform }.join
  end

  private

  def perform
    while (job = @queue.pop)
      break if job == :done
      job.call
    end
  end
end

describe Worker do
  it 'has a name and an inbox queue that shows the count of jobs assigned' do
    worker = Worker.new('worker_1')

    assert_equal 'worker_1', worker.name
    assert_equal 0, worker.jobs_count
  end

  it 'accepts jobs to be queued' do
    worker = Worker.new('worker_1')
    worker << :job
    worker << :job

    assert_equal 2, worker.jobs_count
  end

  it 'performs a callable job asynchronously' do
    worker = Worker.new('worker_1')
    results = []

    Thread.new do
      worker << -> { results.push('received job 1') }
      worker << -> { results.push('received job 2') }
      worker << :done
    end

    assert_equal [], results
    worker.run
    assert_equal ['received job 1', 'received job 2'], results
  end
end

class WorkerPool
  def initialize(num_workers)
    @workers = Array.new(num_workers) { |n| Worker.new("worker_#{n}") }
    @current_worker = @workers.cycle
  end

  def status
    @workers.map(&:jobs_count)
  end

  def <<(job)
    if job == :done
      @workers.map { |w| w << :done }
    else
      @current_worker.next << job
    end
  end

  def run
    @workers.map(&:run)
  end
end

describe WorkerPool do
  it 'has an initial empty status showing the distribution of the jobs' do
    pool = WorkerPool.new(4)

    assert_equal [0, 0, 0, 0], pool.status
  end

  it 'distributes the work using the default Round Robin strategy' do
    pool = WorkerPool.new(4)

    pool << :job
    assert_equal [1, 0, 0, 0], pool.status

    pool << :job
    assert_equal [1, 1, 0, 0], pool.status

    pool << :job
    assert_equal [1, 1, 1, 0], pool.status

    pool << :job
    assert_equal [1, 1, 1, 1], pool.status

    pool << :job
    assert_equal [2, 1, 1, 1], pool.status
  end

  it 'allocates jobs to the workers and run them in parallel' do
    pool = WorkerPool.new(2)
    results = []

    Thread.new do
      10.times do |n|
        pool << -> { 
          `curl https://www.google.ie/search?q=#{n}`
        }
      end
      pool << :done
    end

    assert_equal [], results
    pool.run
    assert_equal 100, results.size
  end
end

