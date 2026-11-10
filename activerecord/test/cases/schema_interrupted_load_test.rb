# frozen_string_literal: true

require "cases/helper"

class SchemaInterruptedLoadTest < ActiveRecord::TestCase
  self.use_transactional_tests = false

  COLUMNS = %w[ id title lock_version created_at updated_at ]

  def test_schema_load_stopped_by_thread_kill_is_retried
    klass, entered = define_model_pausing_in_load
    loader = Thread.new { klass.load_schema }
    assert entered.pop(timeout: 10), "the schema load never reached the pause"

    loader.kill
    assert loader.join(10), "the loader never died"

    assert_equal COLUMNS, klass.new.attribute_names
    assert_equal 1, klass.hook_calls
  ensure
    loader&.kill
  end

  def test_schema_load_stopped_by_thread_kill_during_the_hooks_runs_them_on_next_use
    klass, entered = define_model_pausing_in_hooks
    loader = Thread.new { klass.load_schema }
    assert entered.pop(timeout: 10), "the schema load never reached the pause"

    loader.kill
    assert loader.join(10), "the loader never died"

    klass.new
    assert_equal 2, klass.hook_calls
  ensure
    loader&.kill
  end

  def test_columns_can_be_read_from_another_thread_while_the_hooks_run
    klass, entered, release = define_model_pausing_in_hooks
    loader = Thread.new { klass.load_schema }
    assert entered.pop(timeout: 10), "the schema load never reached the pause"

    reader = Thread.new { klass.columns_hash.keys }
    assert reader.join(10), "the reader waited for the hooks"
    assert_equal COLUMNS, reader.value
  ensure
    release&.push(true)
    loader&.join(10)
  end

  def test_hook_can_wait_on_a_thread_reading_the_columns
    klass = define_model do
      define_singleton_method(:load_schema!) do
        self.hook_calls += 1
        reader = Thread.new { columns_hash.size }
        raise "the reader waited for the hooks" unless reader.join(10)
        super()
      end
    end

    assert_equal COLUMNS, klass.new.attribute_names
    assert_equal 1, klass.hook_calls
  end

  def test_hook_can_read_the_columns_from_another_fiber
    klass = define_model do
      define_singleton_method(:load_schema!) do
        self.hook_calls += 1
        Enumerator.new { |y| y << columns_hash.size }.next
        super()
      end
    end

    assert_equal COLUMNS, klass.new.attribute_names
    assert_equal 1, klass.hook_calls
  end

  if Process.respond_to?(:fork) && !in_memory_db?
    def test_process_forked_during_a_column_load_loads_the_columns
      [true, false].each do |before_publishing|
        klass, entered, release = define_model_pausing_in_load(before_publishing: before_publishing)
        loader = Thread.new { klass.load_schema }
        assert entered.pop(timeout: 10), "the schema load never reached the pause"

        record = in_forked_process do
          record = klass.create!(title: "forked")
          [record.attribute_names, klass.find(record.id).title, klass.hook_calls]
        end
        assert_equal [COLUMNS, "forked", 1], record, "paused before publishing: #{before_publishing}"
      ensure
        release&.push(true)
        loader&.join(10)
      end
    end

    def test_process_forked_during_the_hooks_runs_the_hooks
      klass, entered, release = define_model_pausing_in_hooks
      loader = Thread.new { klass.load_schema }
      assert entered.pop(timeout: 10), "the schema load never reached the pause"

      hook_calls = in_forked_process do
        klass.new
        klass.hook_calls
      end
      assert_equal 2, hook_calls
    ensure
      release&.push(true)
      loader&.join(10)
    end
  end

  private
    def define_model(&block)
      Class.new(ActiveRecord::Base) do
        self.table_name = :lock_without_defaults

        class << self
          attr_accessor :hook_calls
        end
        self.hook_calls = 0

        class_eval(&block) if block
      end
    end

    def define_model_pausing_in_load(before_publishing: false)
      entered, release = Queue.new, Queue.new
      paused = false
      pause = -> do
        unless paused
          paused = true
          entered << true
          release.pop
        end
      end
      pausing_context = Class.new(ActiveRecord::ModelSchema::SchemaContext) do
        define_method(:load_schema!) do |&publish_loading|
          pause.call if before_publishing
          super() do |loading|
            publish_loading.call(loading)
            pause.call
          end
        end
      end

      klass = define_model do
        define_singleton_method(:build_schema_context) { pausing_context.new(self) }
        define_singleton_method(:load_schema!) do
          self.hook_calls += 1
          super()
        end
      end
      [klass, entered, release]
    end

    def define_model_pausing_in_hooks
      entered, release = Queue.new, Queue.new
      paused = false
      klass = define_model do
        define_singleton_method(:load_schema!) do
          self.hook_calls += 1
          unless paused
            paused = true
            entered << true
            release.pop
          end
          super()
        end
      end
      [klass, entered, release]
    end

    # Runs the block in a forked process and returns its result, or fails with what it raised.
    def in_forked_process
      reader, writer = IO.pipe
      reader.binmode
      writer.binmode

      pid = fork do
        reader.close
        exit_code = 0
        begin
          writer.write Marshal.dump(yield)
        rescue Exception => error
          writer.write Marshal.dump(error.full_message(highlight: false).lines.first(3).join)
          exit_code = 1
        end
        writer.close
        exit!(exit_code)
      end
      writer.close

      assert reader.wait_readable(10), "the forked process never finished"
      result = Marshal.load(reader.read)
      _, status = Process.wait2(pid)
      flunk result unless status.success?
      result
    ensure
      if pid && !status
        Process.kill(:KILL, pid)
        Process.wait(pid)
      end
      reader&.close
    end
end
