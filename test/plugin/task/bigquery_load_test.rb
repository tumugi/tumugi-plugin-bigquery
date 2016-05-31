require_relative '../../test_helper'
require 'tumugi/plugin/task/bigquery_load'

class Tumugi::Plugin::BigqueryLoadTaskTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  setup do
    @klass = Class.new(Tumugi::Plugin::BigqueryLoadTask)
    @klass.param_set :bucket, 'tumugi-plugin-bigquery'
    @klass.param_set :key, 'test.csv'
    @klass.param_set :dataset_id, Tumugi::Plugin::BigqueryTestHelper::TEST_DATASETS[0]
    @klass.param_set :table_id, 'load_test'
    @klass.param_set :skip_leading_rows, 1
    @klass.param_set :schema, [
      {
        name: 'row_number',
        type: 'INTEGER',
        mode: 'NULLABLE'
      },
      {
        name: 'value',
        type: 'INTEGER',
        mode: 'NULLABLE'
      },
    ]
  end

  sub_test_case "parameters" do
    test "should set correctly" do
      task = @klass.new
      assert_equal('tumugi-plugin-bigquery', task.bucket)
      assert_equal('test.csv', task.key)
      assert_equal(nil, task.project_id)
      assert_equal(TEST_DATASETS[0], task.dataset_id)
      assert_equal('load_test', task.table_id)
      assert_equal(60, task.wait)
    end

    data({
      "bucket" => [:bucket],
      "key" => [:key],
      "dataset_id" => [:dataset_id],
      "table_id" => [:table_id],
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
    assert_equal('load_test', output.table_id)
  end

  test "#run" do
    task = @klass.new
    output = task.output
    task.run
    result = output.client.list_tabledata(task.dataset_id, task.table_id, project_id: task.project_id)
    assert_equal(5, result[:total_rows])

    expected = [
      {"row_number"=>"1", "value"=>"1"},
      {"row_number"=>"2", "value"=>"2"},
      {"row_number"=>"3", "value"=>"3"},
      {"row_number"=>"4", "value"=>"4"},
      {"row_number"=>"5", "value"=>"5"}
    ]
    assert_equal(expected, result[:rows])
  end
end
