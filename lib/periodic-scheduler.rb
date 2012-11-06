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
		attr_reader :quantum_period
    attr_reader :period
    attr_reader :keep
    attr_reader :callback

    def initialize(quantized_space, now, period, keep, &callback)
			@quantized_space = quantized_space
      @period = period
			@run_time = now + period
      @keep = keep
      @callback = callback
			quantatize(period)
    end

		def reschedule(qnow)
			# keep rescheduling until we get it scheduled in future quant
			until @quantum_period > qnow
				@run_time += @period
				quantatize(@run_time)
			end

			@reschedule_hook.call(self) if @reschedule_hook
		end

		def reschedule_hook(&callback)
			@reschedule_hook = callback
		end

		def keep?
			@keep
		end

		def stop
			return if @stopped
			@stopped = true
			@stop_hook.call(self) if @reschedule_hook
		end

		def stopped?
			@stopped
		end

		def stop_hook(&callback)
			@stop_hook = callback
		end

    def call
      @callback.call
    end

		private

		def quantatize(period)
			@quantum_period = @quantized_space.project(period)
		end
  end

  def initialize(quantum = 5.0, options = {})
    time_source = (options[:time_source] or lambda {Time.now.to_f})
    wait_function = (options[:wait_function] or lambda{|t| sleep t})

    @quantized_space = RealTimeToQuantizedSpaceProjection.new(
      quantum,
      lambda {|v| v.ceil} # behave like sleep - never execute too early
    )
    @time_source = time_source
    @wait_function = wait_function

    @events = {}
  end

	def after(period, &callback)
		schedule_event Event.new(@quantized_space, real_now, period, false, &callback)
	end

	def every(period, &callback)
		schedule_event Event.new(@quantized_space, real_now, period, true, &callback)
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
		earliest_quant = @events.keys.sort.first
		raise EmptyScheduleError, "no events scheduled" unless earliest_quant

    wait_time = @quantized_space.revers_project(earliest_quant) - real_now
		wait_time = 0 if wait_time < 0
    wait(wait_time)

		objects = []

    qnow = quantized_now

		# move quants to be run away to separate array
    quants = @events.keys.select{|k| k <= qnow}.sort.map{|q| @events.delete(q)}

		# we have missed one or more scheduled quants
		if quants.length > 1
      begin
        # we raise it so it has proper backtrace
        raise MissedScheduleError.new("missed schedule by #{-wait_time} seconds")
      rescue StandardError => error
				yield error if block_given?
      end
    end

		# Call callback for every quant and reschedule if needed
    quants.each do |events|
			# get all events for quantum that are not stopped
      events.each do |e|
        begin
          objects << e.call
        rescue StandardError => error
					# Yield errors to block
					yield error if block_given?
        end
        e.reschedule(quantized_now) if e.keep? and not e.stopped?
      end
    end

		# return collected callabck return objects
		objects
  end

  def empty?
		@events.empty?
  end

  private

	def schedule_event(event)
		quant = event.quantum_period
		(@events[quant] ||= []) << event

		event.reschedule_hook do |event|
			unschedule_event(event, quant)
			schedule_event(event)
		end

		event.stop_hook do |event|
			unschedule_event(event, quant)
		end

		event
	end

	def unschedule_event(event, quant)
		return unless @events[quant]
		@events[quant].delete(event)
		@events.delete(quant) if @events[quant].empty?
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

