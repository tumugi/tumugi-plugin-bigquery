require_relative '../../test_helper'
require 'tumugi/plugin/task/bigquery_copy'

class Tumugi::Plugin::BigqueryCopyTaskTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  class ExistTarget < Tumugi::Target
    def exist?; true;end
  end

  class NotExistTarget < Tumugi::Target
    def exist?; false; end
  end

  setup do
    @klass = Class.new(Tumugi::Plugin::BigqueryCopyTask)
    @klass.param_set :src_project_id, 'publicdata'
    @klass.param_set :src_dataset_id, 'samples'
    @klass.param_set :src_table_id, 'shakespeare'
    @klass.param_set :dest_dataset_id, Tumugi::Plugin::BigqueryTestHelper::TEST_DATASETS[0]
    @klass.param_set :dest_table_id, 'test'
  end

  sub_test_case "parameters" do
    test "should set correctly" do
      task = @klass.new
      assert_equal('publicdata', task.src_project_id)
      assert_equal('samples', task.src_dataset_id)
      assert_equal('shakespeare', task.src_table_id)
      assert_equal(nil, task.dest_project_id)
      assert_equal(TEST_DATASETS[0], task.dest_dataset_id)
      assert_equal('test', task.dest_table_id)
      assert_equal(false, task.force_copy)
      assert_equal(60, task.wait)
    end

    data({
      "src_dataset_id" => [:src_dataset_id],
      "src_table_id" => [:src_table_id],
      "dest_dataset_id" => [:dest_dataset_id],
      "dest_table_id" => [:dest_table_id],
    })
    test "raise error when required parameter is not set" do |params|
      params.each do |param|
        @klass.param_set(param, nil)
      end
      assert_raise(Tumugi::ParameterError) do
        @klass.new
      end
    end
  end

  test "#output" do
    task = @klass.new
    output = task.output
    assert_true(output.is_a? Tumugi::Plugin::BigqueryTableTarget)
    assert_equal(ENV['PROJECT_ID'], output.project_id)
    assert_equal(Tumugi::Plugin::BigqueryTestHelper::TEST_DATASETS[0], output.dataset_id)
    assert_equal('test', output.table_id)
  end

  data({
    "force_copy is false with completed state and exist target" => [false, ExistTarget, :completed, true],
    "force_copy is false with completed state and not exist target" => [false, NotExistTarget, :completed, false],
    "force_copy is true with pending state and exist target" => [true, ExistTarget, :pending, false],
    "force_copy is true with pending state and not exist target" => [true, NotExistTarget, :pending, false],
    "force_copy is true with completed state and exist target" => [true, ExistTarget, :completed, true],
    "force_copy is true with completed state and not exist target" => [true, NotExistTarget, :completed, false],
  })
  test "#complted?" do |(force_copy, target_klass, state, expected)|
    @klass.param_set :force_copy, force_copy
    @klass.send(:define_method, :output) do
      target_klass.new
    end
    task = @klass.new
    task.instance_variable_set(:@state, state)
    assert_equal(expected, task.completed?)
  end

  test "#run" do
    task = @klass.new
    output = task.output
    task.run
    src = output.client.list_tabledata(task.src_dataset_id, task.src_table_id, project_id: task.src_project_id)
    dest = output.client.list_tabledata(output.dataset_id, output.table_id)
    assert_equal(src[:total_rows], dest[:total_rows])
    assert_equal(src[:rows], dest[:rows])
  end
end
