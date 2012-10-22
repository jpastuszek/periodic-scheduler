require 'quantized_time_space'
require 'set'

class PeriodicScheduler
  class MissedScheduleError < RuntimeError; end
  class EmptyScheduleError < RuntimeError; end

  class Event
    attr_reader :period
    attr_reader :reschedule
    attr_reader :group
    attr_reader :callback

    def initialize(period, reschedule, group, callback)
      @period = period
      @reschedule = reschedule
      @group = group
      @callback = callback
    end

    def call
      @callback.call
    end
  end

  class QuantizedEventBuilder
    class QuantizedEvent < Event
      attr_reader :quantum_period
      attr_reader :quantum_error

      def initialize(period, reschedule, group, callback, quantum_period, quantum_error)
        super(period, reschedule, group, callback)
        @quantum_period = quantum_period
        @quantum_error = quantum_error
      end
    end

    def initialize(quantized_space)
      @quantized_space = quantized_space
    end

    def from_event(event)
      QuantizedEvent.new(
        event.period,
        event.reschedule,
        event.group,
        event.callback,
        @quantized_space.project(event.period),
        @quantized_space.projection_error(event.period)
      )
    end

    def reschedule(quantized_event)
      accumulated_period = quantized_event.period + quantized_event.quantum_error
      QuantizedEvent.new(
        quantized_event.period,
        quantized_event.reschedule,
        quantized_event.group,
        quantized_event.callback,
        @quantized_space.project(accumulated_period),
        @quantized_space.projection_error(accumulated_period)
      )
    end
  end


  def initialize(quantum = 5.0, options = {})
    time_source = (options[:time_source] or lambda {Time.now.to_f})
    wait_function = (options[:wait_function] or lambda{|t| sleep t})

    @quantized_space = RealTimeToQuantizedSpaceProjection.new(
      quantum,
      lambda {|v| v.floor}
    )
    @quantized_event_builder = QuantizedEventBuilder.new(@quantized_space)
    @time_source = time_source
    @wait_function = wait_function

    @events = {}
    @event_groups_to_unschedule = Set.new
  end

  def schedule(period, reschedule = false, group = nil, &callback)
    event = @quantized_event_builder.from_event(Event.new(period, reschedule, group, callback))
    period = quantized_now + event.quantum_period
    add_event(event, period)
  end

  def unschedule_group(group)
    @event_groups_to_unschedule << group
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
    process_unsheduled_events

    earliest_quant = @events.keys.sort[0]
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

    # It may happen that wait returned qucker than it should
		# In this case just return no data
    if quants.empty?
      return objects
    end

		# Call callback for every quant and reschedule if needed
    quants.each do |q|
      events = @events[q]
      @events.delete(q)
      events.each do |e|
        begin
          objects << e.call
        rescue StandardError => ex
          errors << ex
        end
        reschedule_event(e, q) if e.reschedule
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
    # do the cleanup - this may be causing problems!
    process_unsheduled_events

    @events.empty?
  end

  private

  def process_unsheduled_events
    return if @event_groups_to_unschedule.empty?

    new_events = {}

    @events.each_pair do |quant, events|
      evs = events.select do |event|
        not @event_groups_to_unschedule.member?(event.group)
      end

      new_events[quant] = evs unless evs.empty?
    end

    @events = new_events
    @event_groups_to_unschedule = Set.new
  end

  def add_event(quantized_event, period)
    @events[period] = [] unless @events[period]
    @events[period] << quantized_event
  end

  def reschedule_event(quantized_event, previous_run_quant)
    event = @quantized_event_builder.reschedule(quantized_event)
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

