require_relative '../../../test_helper'
require 'tumugi/plugin/target/bigquery_table'

class Tumugi::Plugin::BigqueryTableTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  test "#initialize" do
    target = Tumugi::Plugin::BigqueryTableTarget.new(project_id: credential[:project_id], dataset_id: TEST_DATASETS[0], table_id: 'test')
    assert_equal(credential[:project_id], target.project_id)
    assert_equal(TEST_DATASETS[0], target.dataset_id)
    assert_equal('test', target.table_id)
  end

  sub_test_case "#exist?" do
    test "return true if exists" do
      target = Tumugi::Plugin::BigqueryTableTarget.new(project_id: credential[:project_id], dataset_id: TEST_DATASETS[0], table_id: 'test')
      assert_true(target.exist?)
    end

    test "return false if not exists" do
      target = Tumugi::Plugin::BigqueryTableTarget.new(project_id: credential[:project_id], dataset_id: TEST_DATASETS[0], table_id: 'not_found')
      assert_false(target.exist?)
    end
  end

  test "to_s" do
    target = Tumugi::Plugin::BigqueryTableTarget.new(project_id: credential[:project_id], dataset_id: TEST_DATASETS[0], table_id: 'test')
    assert_equal("bq://#{credential[:project_id]}/#{TEST_DATASETS[0]}/test", target.to_s)
  end
end
