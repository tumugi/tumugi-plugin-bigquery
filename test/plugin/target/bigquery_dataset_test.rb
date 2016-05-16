require_relative '../../test_helper'
require 'tumugi/plugin/target/bigquery_dataset'

class Tumugi::Plugin::BigqueryDatasetTargetTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  test "#initialize" do
    target = Tumugi::Plugin::BigqueryDatasetTarget.new(project_id: credential[:project_id], dataset_id: TEST_DATASETS[0])
    assert_equal(credential[:project_id], target.project_id)
    assert_equal(TEST_DATASETS[0], target.dataset_id)
  end

  sub_test_case "#exist?" do
    test "return true if exists" do
      target = Tumugi::Plugin::BigqueryDatasetTarget.new(project_id: credential[:project_id], dataset_id: TEST_DATASETS[0])
      assert_true(target.exist?)
    end

    test "return false if not exists" do
      target = Tumugi::Plugin::BigqueryDatasetTarget.new(project_id: credential[:project_id], dataset_id: 'not_found')
      assert_false(target.exist?)
    end
  end

  test "to_s" do
    target = Tumugi::Plugin::BigqueryDatasetTarget.new(project_id: credential[:project_id], dataset_id: TEST_DATASETS[0])
    assert_equal("bq://#{credential[:project_id]}/#{TEST_DATASETS[0]}", target.to_s)
  end
end
