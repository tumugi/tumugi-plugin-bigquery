require_relative '../../test_helper'
require 'tumugi/plugin/task/bigquery_query'

class Tumugi::Plugin::BigqueryQueryTaskTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  class ExistTarget < Tumugi::Target
    def exist?; true;end
  end

  class NotExistTarget < Tumugi::Target
    def exist?; false; end
  end

  setup do
    @klass = Class.new(Tumugi::Plugin::BigqueryQueryTask)
    @klass.set :query, "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
    @klass.set :dataset_id, Tumugi::Plugin::BigqueryTestHelper::TEST_DATASETS[0]
    @klass.set :table_id, 'test'
    @klass.set :project_id, ENV['PROJECT_ID']
  end

  sub_test_case "parameters" do
    test "should set correctly" do
      task = @klass.new
      assert_equal("SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]", task.query)
      assert_equal(TEST_DATASETS[0], task.dataset_id)
      assert_equal('test', task.table_id)
      assert_equal(ENV['PROJECT_ID'], task.project_id)
      assert_equal('truncate', task.mode)
      assert_equal(true, task.flatten_results)
      assert_equal(true, task.use_legacy_sql)
      assert_equal(60, task.wait)
    end

    data({
      "query" => [:query],
      "dataset" => [:dataset_id],
      "table" => [:table_id],
    })
    test "raise error when required parameter is not set" do |params|
      params.each do |param|
        @klass.set(param, nil)
      end
      assert_raise(Tumugi::ParameterError) do
        @klass.new
      end
    end
  end

  data({
    "truncate mode with completed state and exist target" => ["truncate", ExistTarget, :completed, true],
    "truncate mode with completed state and not exist target" => ["truncate", NotExistTarget, :completed, false],
    "append mode with pending state and exist target" => ["append", ExistTarget, :pending, false],
    "append mode with pending state and not exist target" => ["append", NotExistTarget, :pending, false],
    "append mode with completed state and exist target" => ["append", ExistTarget, :completed, true],
    "append mode with completed state and not exist target" => ["append", NotExistTarget, :completed, false],
  })
  test "#completed?" do |(mode, target_klass, state, expected)|
    @klass.set :mode, mode
    @klass.send(:define_method, :output) do
      target_klass.new
    end
    task = @klass.new
    task.instance_variable_set(:@state, state)
    assert_equal(expected, task.completed?)
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

  test "#run with append mode" do
    @klass.set :table_id, 'test_append'
    @klass.set :mode, 'append'
    task = @klass.new
    output = task.output
    task.run
    result = output.client.list_tabledata(output.dataset_id, task.table_id)
    assert_equal({:total_rows=>1, :next_token=>nil, :rows=>[{"cnt"=>"313797035"}]}, result)

    task.run
    result = output.client.list_tabledata(output.dataset_id, task.table_id)
    assert_equal({:total_rows=>2, :next_token=>nil, :rows=>[{"cnt"=>"313797035"}, {"cnt"=>"313797035"}]}, result)
  end
end
