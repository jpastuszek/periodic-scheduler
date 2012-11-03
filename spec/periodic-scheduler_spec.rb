require 'periodic-scheduler'

describe PeriodicScheduler do
  before :each do
    @time_now = 0
    @options = {
      :time_source => lambda{@time_now},
      :wait_function => lambda{|t| 
        @time_now += t
        #puts "sleeping for #{t}"
      }
    }

    @got_events = []
    @got_event = lambda{|no|
      #puts "event #{no} at #{@time_now}"
      @got_events << no
    }
  end

  it "should execut event callbacks given time progress" do
    s = PeriodicScheduler.new(5.0, @options)

    s.after(9.5) do
      @got_event.call(1)
    end

    s.after(14) do
      @got_event.call(2)
    end

    s.after(20) do
      @got_event.call(3)
    end

    @got_events.should == []
    s.empty?.should == false

    s.run
    @got_events.should == [1]
    @time_now.should == 10.0

    s.run
    @got_events.should == [1, 2]
    @time_now.should == 15.0

		s.run
		@got_events.should == [1, 2, 3]
		@time_now.should == 20.0

    s.empty?.should == true
  end

  it "should reschedule resheduable tasks" do
    s = PeriodicScheduler.new(5.0, @options)

    s.every(15) do
      @got_event.call(1)
    end

    @got_events.should == []

    s.run
    @got_events.should == [1]
    @time_now.should == 15.0

    s.run
    @got_events.should == [1, 1]
    @time_now.should == 30

    s.run
    @got_events.should == [1, 1, 1]
    @time_now.should == 45
  end

  it "should compensate for quntization error" do
    s = PeriodicScheduler.new(5.0, @options)

    s.every(12) do
      @got_event.call(1)
    end

    @got_events.should == []

    s.run
    @got_events.should == [1]
    @time_now.should == 15

    s.run
    @got_events.should == [1, 1]
    @time_now.should == 25

    s.run
    @got_events.should == [1, 1, 1]
    @time_now.should == 40

    s.run
    @got_events.should == [1, 1, 1, 1]
    @time_now.should == 50

    s.run
    @got_events.should == [1, 1, 1, 1, 1]
    @time_now.should == 60

    s.run
    @got_events.should == [1, 1, 1, 1, 1, 1]
    @time_now.should == 75
  end

  it "should compensate for wait function jitter" do
    jitter = [1, 0, 5, -5, 0.5, -0.2, 0, 0, 0]
    @options[:wait_function] = lambda{|t| 
      j = jitter.shift
      #puts "time is: #{@time_now}"
      @time_now += t + j
      #puts "sleeping for #{t} + jitter #{j}: #{t + j}"
    }

    s = PeriodicScheduler.new(5.0, @options)

    s.every(12) do
      @got_event.call(1)
    end

    @got_events.should == []

    s.run.should_not be_empty
		@time_now.should == 16
    @got_events.should == [1]

    s.run.should_not be_empty
		@time_now.should == 25
    @got_events.should == [1, 1]

    s.run.should_not be_empty
		@time_now.should == 45
    @got_events.should == [1, 1, 1]

		# if timer returns too quickly the run will be empty
    s.run.should be_empty
		@time_now.should == 45
		@got_events.should == [1, 1, 1]
		
		s.run.should_not be_empty
		@time_now.should == 50.5
    @got_events.should == [1, 1, 1, 1]

		# now the jitter is too small to not progress the quant
    s.run.should_not be_empty
		@time_now.should == 59.8
    @got_events.should == [1, 1, 1, 1, 1]

    s.run.should_not be_empty
		@time_now.should == 75.0
    @got_events.should == [1, 1, 1, 1, 1, 1]

    s.run.should_not be_empty
		@time_now.should == 85.0
    @got_events.should == [1, 1, 1, 1, 1, 1, 1]
  end

  it "should keep average scheduling precision over longer time" do
    srand(100) # make rand deterministic
    @options[:wait_function] = lambda{|t| 
      j = (rand - 0.5) * 10
      @time_now += t + j
    }

    ev1_val = Math::PI * 10
    ev1 = []
    ev1_last = 0

    ev2_val = Math::E * 5
    ev2 = []
    ev2_last = 0

    s = PeriodicScheduler.new(5.0, @options) 

    s.every(ev1_val) do
      ev1 << @time_now - ev1_last
      ev1_last = @time_now
    end

    s.every(ev2_val) do
      ev2 << @time_now - ev2_last
      ev2_last = @time_now
    end

    10000.times{ s.run }
    
    (ev1.inject(0){|v, s| v + s} / ev1.length).should be_within(0.001).of(ev1_val)
    (ev2.inject(0){|v, s| v + s} / ev2.length).should be_within(0.001).of(ev2_val)
  end

  it "should support unscheduling of events" do
    s = PeriodicScheduler.new(5.0, @options)

    e1 = s.every(15) do
      @got_event.call(1)
    end

    e2 = s.every(20) do
      @got_event.call(2)
    end

    e3 = s.every(25) do
      @got_event.call(3)
    end

    @got_events.should == []

    s.run
    @got_events.should == [1]
    @time_now.should == 15

    s.run
    @got_events.should == [1, 2]
    @time_now.should == 20

    s.run
    @got_events.should == [1, 2, 3]
    @time_now.should == 25

    s.run
    @got_events.should == [1, 2, 3, 1]
    @time_now.should == 30

		e1.stop
		e2.stop

    s.run
    @got_events.should == [1, 2, 3, 1, 3]
		@time_now.should == 50

    s.empty?.should == false

		e3.stop
    s.empty?.should == true
  end

  it "should support unscheduling of events from other event" do
    s = PeriodicScheduler.new(1.0, @options)

    e1 = s.every(1) do
      @got_event.call(1)
    end

    e2 = s.every(1) do
      @got_event.call(2)
    end

    e3 = s.every(1) do
      @got_event.call(3)
    end

		e4 = s.every(2) do
			e1.stop
			@got_event.call(4)
		end

    @got_events.should == []

    s.run
    @got_events.should == [1, 2, 3]
    @time_now.should == 1

    # Will get executed this time
    s.run
    @got_events.should == [1, 2, 3, 4, 1, 2, 3]
    @time_now.should == 2

    # Should be not rescheduled
    s.run
    @got_events.should == [1, 2, 3, 4, 1, 2, 3, 2, 3]
    @time_now.should == 3
  end

  it "should execut all not reschedulable tasks if we miss them" do
    s = PeriodicScheduler.new(5.0, @options)

    s.after(15) do
      @got_event.call(1)
    end

    s.after(30) do
      @got_event.call(2)
    end

    s.after(45) do
      @got_event.call(3)
    end

    @options[:wait_function].call(35)

    s.run.should
    @got_events.should == [1, 2]

    s.run.should
    @got_events.should == [1, 2, 3]
  end

  it "should skpi reschedulable tasks if we miss them" do
    #TODO: this behaviour is a bit of gary area
    s = PeriodicScheduler.new(5.0, @options)

    s.every(15) do
      @got_event.call(1)
    end

    @options[:wait_function].call(35)

    s.run.should
    @got_events.should == [1]

    s.run.should
    @got_events.should == [1, 1]
  end

  it "should report error if the schedule was missed" do
    s = PeriodicScheduler.new(5.0, @options)

    s.after(15) do
      @got_event.call(1)
    end

    s.after(30) do
      @got_event.call(2)
    end

    @options[:wait_function].call(35)

		errors = []
		s.run do |error|
			errors << error
		end
		
    errors.should_not be_empty
    errors[0].class.should == PeriodicScheduler::MissedScheduleError
    @got_events.should == [1, 2]
  end

  describe "run" do
    it "should handle events call exceptions and return them" do
      s = PeriodicScheduler.new(5.0, @options)

      s.every(12) do
        fail "test"
      end

      errors = []

      lambda {
				s.run do |error|
					errors << error
				end
      }.should_not raise_exception

      errors.should have(1).error
      errors[0].should be_kind_of RuntimeError
      errors[0].to_s.should == "test"
    end

    it "should raise PeriodicScheduler::EmptyScheduleError if there are no events left to process" do
      s = PeriodicScheduler.new(5.0, @options)

      s.after(12) {}

      lambda {
        s.run
      }.should_not raise_error

      lambda {
        s.run
      }.should raise_error PeriodicScheduler::EmptyScheduleError
    end

		it "should provide array of objects returned by called shedule blocks" do
			s = PeriodicScheduler.new(5.0, @options)

			test = 0

			s.every(11) do
				test += 1
			end

			s.after(12) do
				test += 1
			end

			s.run.should == [1, 2]
			s.run.should == [3]
		end
  end

  describe "#run!" do
    it "should run schedule until there is no more events sheduled" do
      s = PeriodicScheduler.new(5.0, @options)

      s.after(5) do
        @got_event.call(1)
      end

      s.after(10) do
        @got_event.call(2)
      end

      s.after(15) do
        @got_event.call(3)
      end

      s.run!

      @got_events.should == [1, 2, 3]
    end

    it "should call block on event error" do
      s = PeriodicScheduler.new(5.0, @options)

      s.after(5) do
        raise "1"
      end

      s.after(10) do
        @got_event.call(1)
      end

      s.after(15) do
        raise "2"
      end

      s.after(15) do
        @got_event.call(2)
      end

      @got_fails = []
      s.run! do |error|
        error.should be_kind_of RuntimeError
        @got_fails << error.message
      end

      @got_fails.map{|m| m.to_i}.should == [1, 2]
      @got_events.should == [1, 2]
    end
  end
end

