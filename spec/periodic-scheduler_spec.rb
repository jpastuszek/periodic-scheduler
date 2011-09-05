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

    s.schedule(11.5) do
      @got_event.call(1)
    end

    s.schedule(14) do
      @got_event.call(2)
    end

    s.schedule(20) do
      @got_event.call(3)
    end

    @got_events.should == []

    s.wait_events
    @got_events.should == [1, 2]
    @time_now.should == 10.0

    s.wait_events
    @got_events.should == [1, 2, 3]
    @time_now.should == 20.0

    # They should not get resheduled by default
    s.wait_events
    @got_events.should == [1, 2, 3]

    s.wait_events
    @got_events.should == [1, 2, 3]
  end

  it "should reschedule resheduable tasks" do
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(15, true) do
      @got_event.call(1)
    end

    @got_events.should == []

    s.wait_events
    @got_events.should == [1]
    @time_now.should == 15.0

    s.wait_events
    @got_events.should == [1, 1]
    @time_now.should == 30

    s.wait_events
    @got_events.should == [1, 1, 1]
    @time_now.should == 45
  end

  it "should compensate for quntization error" do
    s = PeriodicScheduler.new(5.0, @options)

    # Note that now we are using floor to quantize event
    s.schedule(12, true) do
      @got_event.call(1)
    end

    @got_events.should == []

    s.wait_events
    @got_events.should == [1]
    @time_now.should == 10

    s.wait_events
    @got_events.should == [1, 1]
    @time_now.should == 20

    s.wait_events
    @got_events.should == [1, 1, 1]
    @time_now.should == 35

    s.wait_events
    @got_events.should == [1, 1, 1, 1]
    @time_now.should == 45

    s.wait_events
    @got_events.should == [1, 1, 1, 1, 1]
    @time_now.should == 60

    s.wait_events
    @got_events.should == [1, 1, 1, 1, 1, 1]
    @time_now.should == 70
  end

  it "should compensate for wait function jitter" do
    jitter = [1, 0, 5, -1, 0.5, -0.2, 0, 0, 0]
    @options[:wait_function] = lambda{|t| 
      j = jitter.shift
      #puts "time is: #{@time_now}"
      @time_now += t + j
      #puts "sleeping fou #{t} + jitter #{j}: #{t + j}"
    }

    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(12, true) do
      @got_event.call(1)
    end

    @got_events.should == []

    s.wait_events
    @got_events.should == [1]
    @time_now.should == 11

    s.wait_events
    @got_events.should == [1, 1]
    @time_now.should == 20

    s.wait_events
    @got_events.should == [1, 1, 1]
    @time_now.should == 40

    s.wait_events
    @got_events.should == [1, 1, 1, 1]
    @time_now.should == 45.5

    s.wait_events
    @got_events.should == [1, 1, 1, 1, 1]
    @time_now.should == 60

    s.wait_events
    @got_events.should == [1, 1, 1, 1, 1, 1]
    @time_now.should == 70.0

    s.wait_events
    @got_events.should == [1, 1, 1, 1, 1, 1, 1]
    @time_now.should == 80.0
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

    s.schedule(ev1_val, true) do
      ev1 << @time_now - ev1_last
      ev1_last = @time_now
    end

    s.schedule(ev2_val, true) do
      ev2 << @time_now - ev2_last
      ev2_last = @time_now
    end

    10000.times{ s.wait_events }
    
    (ev1.inject(0){|v, s| v + s} / ev1.length).should be_within(0.001).of(ev1_val)
    (ev2.inject(0){|v, s| v + s} / ev2.length).should be_within(0.001).of(ev2_val)
  end

  it "should support grouping of events" do
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(12, true, "test group") {}
  end

  it "should support unscheduling of all events within a group" do
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(15, true, "g1") do
      @got_event.call(1)
    end

    s.schedule(20, true, "g1") do
      @got_event.call(2)
    end

    s.schedule(25, true, "g2") do
      @got_event.call(3)
    end

    @got_events.should == []

    s.wait_events
    @got_events.should == [1]
    @time_now.should == 15

    s.wait_events
    @got_events.should == [1, 2]
    @time_now.should == 20

    s.wait_events
    @got_events.should == [1, 2, 3]
    @time_now.should == 25

    s.wait_events
    @got_events.should == [1, 2, 3, 1]
    @time_now.should == 30

    s.unschedule_group("g1")

    s.wait_events
    @got_events.should == [1, 2, 3, 1, 3]
    @time_now.should == 50

    s.unschedule_group("g2")

    s.wait_events.should be_nil
    @got_events.should == [1, 2, 3, 1, 3]
    @time_now.should == 50
  end

  it "should support unscheduling of all events within a group from other event" do
    s = PeriodicScheduler.new(1.0, @options)

    s.schedule(2, true, "g4") do
      s.unschedule_group("g2")
      @got_event.call(4)
    end

    s.schedule(1, true, "g1") do
      @got_event.call(1)
    end

    s.schedule(1, true, "g2") do
      @got_event.call(2)
    end

    s.schedule(1, true, "g2") do
      @got_event.call(3)
    end

    @got_events.should == []

    s.wait_events
    @got_events.should == [1, 2, 3]
    @time_now.should == 1

    # They will get executed this time
    s.wait_events
    @got_events.should == [1, 2, 3, 4, 1, 2, 3]
    @time_now.should == 2

    # They should be not rescheduled and gone
    s.wait_events
    @got_events.should == [1, 2, 3, 4, 1, 2, 3, 1]
    @time_now.should == 3
  end

  it "should execut all not reschedulable tasks if we miss them" do
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(15) do
      @got_event.call(1)
    end

    s.schedule(30) do
      @got_event.call(2)
    end

    s.schedule(45) do
      @got_event.call(3)
    end

    @options[:wait_function].call(35)

    s.wait_events.should
    @got_events.should == [1, 2]

    s.wait_events.should
    @got_events.should == [1, 2, 3]
  end

  it "should skpi reschedulable tasks if we miss them" do
    #TODO: this behaviour is a bit of gary area
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(15, true) do
      @got_event.call(1)
    end

    @options[:wait_function].call(35)

    s.wait_events.should
    @got_events.should == [1]

    s.wait_events.should
    @got_events.should == [1, 1]
  end

  it "should report error if the schedule was missed" do
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(15) do
      @got_event.call(1)
    end

    s.schedule(30) do
      @got_event.call(2)
    end

    @options[:wait_function].call(35)

    errors = s.wait_events
    errors.should_not be_empty
    errors[0].class.should == PeriodicScheduler::MissedScheduleError
    @got_events.should == [1, 2]
  end

end

describe PeriodicScheduler, "wait_events" do
  before :each do
    @time_now = 0
    @options = {
      :time_source => lambda{@time_now},
      :wait_function => lambda{|t| 
        @time_now += t
      }
    }
  end

  it "should handle events call exceptions and return them" do
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(12, true) do
      fail "test"
    end

    errors = nil

    lambda {
      errors = s.wait_events
    }.should_not raise_exception

    errors.should have(1).error
    errors[0].should be_kind_of RuntimeError
    errors[0].to_s.should == "test"
  end

  it "should return nil if there are no events left to process" do
    s = PeriodicScheduler.new(5.0, @options)

    s.schedule(12) {}

    s.wait_events.should_not be_nil
    s.wait_events.should be_nil
  end
end

