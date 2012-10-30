require 'set'

class PeriodicScheduler
  class MissedScheduleError < RuntimeError; end
  class EmptyScheduleError < RuntimeError; end

	class RealTimeToQuantizedSpaceProjection
		def initialize(quantum_size, quantization_rule)
			@quantum_size = quantum_size
			@quantization_rule = quantization_rule
		end

		def project(value)
			@quantization_rule.call(value / @quantum_size)
		end

		def revers_project(value)
			value * @quantum_size
		end

		def projection_error(value)
			value - revers_project(project(value))
		end
	end

  class Event
    attr_reader :period
    attr_reader :keep
    attr_reader :callback
		attr_reader :quantum_period

    def initialize(quantized_space, period, keep, &callback)
      @period = period
      @keep = keep
      @callback = callback
			@quantized_space = quantized_space
			quantatize(period)
    end

		def reschedule
			quantatize(@period + @quantum_error)
		end

		def keep?
			@keep
		end

		def stop
			@stop = true
		end

		def stopped?
			@stop
		end

    def call
      @callback.call
    end

		private

		def quantatize(period)
			@quantum_period = @quantized_space.project(period)
			@quantum_error = @quantized_space.projection_error(period)
		end
  end

  def initialize(quantum = 5.0, options = {})
    time_source = (options[:time_source] or lambda {Time.now.to_f})
    wait_function = (options[:wait_function] or lambda{|t| sleep t})

    @quantized_space = RealTimeToQuantizedSpaceProjection.new(
      quantum,
      lambda {|v| v.floor}
    )
    @time_source = time_source
    @wait_function = wait_function

    @events = {}
  end

  def schedule(period, keep = false, g = nil, &callback)
    event = Event.new(@quantized_space, period, keep, &callback)
    period = quantized_now + event.quantum_period
    add_event(event, period)
  end

  def run!(&block)
    begin
      loop do
        run(&block)
      end
    rescue EmptyScheduleError
    end
  end

  def run
		earliest_quant = find_earliest_quant
		raise EmptyScheduleError, "no events scheduled" unless earliest_quant

    errors = []

    wait_time = @quantized_space.revers_project(earliest_quant) - real_now
    if wait_time < 0
      # we have missed our scheduled period
      begin
        # we raise it so it has proper content (backtrace)
        raise MissedScheduleError.new("missed schedule by #{-wait_time} seconds")
      rescue StandardError => ex
        errors << ex
      end

      wait_time = 0
    end
    wait(wait_time)

		objects = []

    qnow = quantized_now
    quants = @events.keys.select{|k| k <= qnow}.sort

		# Call callback for every quant and reschedule if needed
    quants.each do |q|
			# get all events for quantum that are not stopped
      @events.delete(q).each do |e|
        begin
          objects << e.call
        rescue StandardError => ex
          errors << ex
        end
				# reschedule events unless they are not to be keept or got stopped in the mean time
        reschedule_event(e, q) if e.keep? and not e.stopped?
      end
    end
    
		# Yield errors to block
		if block_given?
			errors.each do |error|
				yield error
			end
		end

		# return collected callabck return objects
		objects
  end

  def empty?
		not find_earliest_quant
  end

  private

	def find_earliest_quant
		# filters quants from oldest to newest until 
		# one that has at least one not stopped event
		@events.keys.sort.each do |quant|
			events = @events[quant]
			events.delete_if{|e| e.stopped?}
			if events.empty?
				@events.delete(quant)
				next
			end
			return quant
		end
		nil
	end
	
  def add_event(quantized_event, period)
    @events[period] = [] unless @events[period]
    @events[period] << quantized_event
		quantized_event
  end

  def reschedule_event(event, previous_run_quant)
		event.reschedule
    period = previous_run_quant + event.quantum_period
    add_event(event, period)
  end

  def wait(time)
    fail "time must be a positive number" if time < 0
    @wait_function.call(time)
  end

  def real_now
    @time_source.call
  end

  def quantized_now
    @quantized_space.project(real_now)
  end
end

