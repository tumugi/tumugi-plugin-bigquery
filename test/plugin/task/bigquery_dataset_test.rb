require_relative '../../test_helper'
require 'tumugi/plugin/task/bigquery_dataset'

class Tumugi::Plugin::BigqueryDatasetTaskTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  setup do
    @klass = Class.new(Tumugi::Plugin::BigqueryDatasetTask)
    @klass.param_set :dataset_id, Tumugi::Plugin::BigqueryTestHelper::TEST_DATASETS[0]
    @klass.param_set :project_id, ENV['PROJECT_ID']
  end

  sub_test_case "parameters" do
    test "should set correctly" do
      task = @klass.new
      assert_equal(TEST_DATASETS[0], task.dataset_id)
      assert_equal(ENV['PROJECT_ID'], task.project_id)
    end

    data({
      "dataset" => [:dataset_id],
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
    assert_true(output.is_a? Tumugi::Plugin::BigqueryDatasetTarget)
    assert_equal(ENV['PROJECT_ID'], output.project_id)
    assert_equal(Tumugi::Plugin::BigqueryTestHelper::TEST_DATASETS[0], output.dataset_id)
  end

  test "#run" do
    task = @klass.new
    output = task.output
    task.run
    assert_not_nil(output.client.dataset(output.dataset_id))
  end
end
