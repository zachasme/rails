# frozen_string_literal: true

require "cases/helper"

module SchemaLoadCounter
  extend ActiveSupport::Concern

  module ClassMethods
    attr_accessor :load_schema_calls

    def load_schema!
      self.load_schema_calls ||= 0
      self.load_schema_calls += 1
      super
    end
  end
end

class SchemaLoadingTest < ActiveRecord::TestCase
  def test_basic_model_is_loaded_once
    klass = define_model
    klass.new
    assert_equal 1, klass.load_schema_calls
  end

  def test_model_with_custom_lock_is_loaded_once
    klass = define_model do |c|
      c.table_name = :lock_without_defaults_cust
      c.locking_column = :custom_lock_version
    end
    klass.new
    assert_equal 1, klass.load_schema_calls
  end

  def test_model_with_changed_custom_lock_is_loaded_twice
    klass = define_model do |c|
      c.table_name = :lock_without_defaults_cust
    end
    klass.new
    klass.locking_column = :custom_lock_version
    klass.new
    assert_equal 2, klass.load_schema_calls
  end

  def test_schema_loading_doesnt_query_when_schema_cache_is_loaded
    with_temporary_connection_pool do
      if in_memory_db?
        # Separate connections to an in-memory database create an entirely new database,
        # with an empty schema etc, so we just stub out this schema on the fly.
        ActiveRecord::Base.connection_pool.with_connection do |connection|
          connection.create_table :tasks do |t|
            t.datetime :starting
            t.datetime :ending
          end
        end
      end

      klass = define_model do |c|
        c.table_name = :tasks
      end

      klass.connection_pool.schema_cache.load!
      klass.connection_pool.schema_cache.add("tasks")
      klass.connection_pool.disconnect!
      klass.send(:reload_schema_from_cache)


      assert_no_queries(include_schema: true) do
        klass.load_schema
      end
      assert_equal 1, klass.load_schema_calls
    end
  end

  def test_schema_load_stopped_by_thread_kill_is_retried
    klass, entered = define_model_pausing_in_load
    loader = Thread.new { klass.load_schema }
    assert entered.pop(timeout: 10), "the schema load never reached the pause"

    context = klass.current_schema_context
    loader.kill
    assert loader.join(10), "the loader never died"

    klass.new
    assert_same context, klass.current_schema_context
    assert_equal 1, klass.load_schema_calls
  ensure
    loader&.kill
  end

  def test_load_schema_hook_can_reset_the_schema_and_read_it
    klass = define_model do |c|
      reset = false
      c.define_singleton_method(:load_schema!) do
        super()
        unless reset
          reset = true
          reload_schema_from_cache(false)
          columns_hash
        end
      end
    end

    assert_equal %w[ id title lock_version created_at updated_at ], klass.new.attribute_names
  end

  def test_load_schema_hook_can_read_the_columns_from_another_fiber
    klass = define_model do |c|
      c.define_singleton_method(:load_schema!) do
        Enumerator.new { |y| y << columns_hash.size }.next
        super()
      end
    end

    klass.new
    assert_equal 1, klass.load_schema_calls
  end

  def test_schema_context_can_be_reloaded_without_its_hooks
    klass = define_model
    klass.new
    klass.send(:reload_schema_contexts_from_cache)

    assert_equal 5, klass.columns_hash.size
    assert_equal 1, klass.load_schema_calls
  end

  if Process.respond_to?(:fork) && !in_memory_db?
    def test_process_forked_during_a_column_load_loads_the_columns
      klass, entered, release = define_model_pausing_in_load
      loader = Thread.new { klass.load_schema }
      assert entered.pop(timeout: 10), "the schema load never reached the pause"

      attribute_names = in_forked_process { klass.new.attribute_names }
      assert_equal %w[ id title lock_version created_at updated_at ], attribute_names
    ensure
      release&.push(true)
      loader&.join(10)
    end
  end

  private
    def define_model
      Class.new(ActiveRecord::Base) do
        include SchemaLoadCounter
        self.table_name = :lock_without_defaults
        yield self if block_given?
      end
    end

    def define_model_pausing_in_load
      entered, release = Queue.new, Queue.new
      paused = false
      paused_context = Class.new(ActiveRecord::ModelSchema::SchemaContext) do
        define_method(:load_schema!) do
          unless paused
            paused = true
            entered << true
            release.pop
          end
          super()
        end
      end

      klass = define_model do |c|
        c.define_singleton_method(:build_schema_context) { paused_context.new(self) }
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
        rescue => error
          writer.write Marshal.dump(error.full_message)
          exit_code = 1
        end
        writer.close
        exit!(exit_code)
      end
      writer.close

      assert reader.wait_readable(10), "the forked process never finished its schema load"
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
