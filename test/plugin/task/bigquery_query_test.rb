require_relative '../../test_helper'
require 'tumugi/plugin/task/bigquery_query'

class Tumugi::Plugin::BigqueryQueryTaskTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  setup do
    @klass = Class.new(Tumugi::Plugin::BigqueryQueryTask)
    @klass.param_set :query, "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
    @klass.param_set :dataset_id, Tumugi::Plugin::BigqueryTestHelper::TEST_DATASETS[0]
    @klass.param_set :table_id, 'test'
  end

  sub_test_case "parameters" do
    test "should set correctly" do
      task = @klass.new
      assert_equal("SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]", task.query)
      assert_equal(TEST_DATASETS[0], task.dataset_id)
      assert_equal('test', task.table_id)
      assert_equal(ENV['PROJECT_ID'], task.project_id)
      assert_equal(60, task.wait)
    end

    data({
      "query" => [:query],
      "dataset" => [:dataset_id],
      "table" => [:table_id],
    })
    test "raise error when required parameter is not set" do |params|
      params.each do |param|
        @klass.param_set(param, nil)
      end
      assert_raise(Tumugi::Parameter::ParameterError) do
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

  test "#run" do
    task = @klass.new
    output = task.output
    task.run
    result = output.client.list_tabledata(output.dataset_id, task.table_id)
    assert_equal({:total_rows=>1, :next_token=>nil, :rows=>[{"cnt"=>"313797035"}]}, result)
  end
end
