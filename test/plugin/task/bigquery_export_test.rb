require_relative '../../test_helper'
require 'tumugi/plugin/task/bigquery_export'

class Tumugi::Plugin::BigqueryExportTaskTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  setup do
    @klass = Class.new(Tumugi::Plugin::BigqueryExportTask)
    @klass.param_set :bucket, 'tumugi-plugin-bigquery'
    @klass.param_set :key, 'export/test.csv.zip'
    @klass.param_set :project_id, 'bigquery-public-data'
    @klass.param_set :dataset_id, 'samples'
    @klass.param_set :table_id, 'shakespeare'
    @klass.param_set :compression, 'GZIP'
  end

  sub_test_case "parameters" do
    test "should set correctly" do
      task = @klass.new
      assert_equal('tumugi-plugin-bigquery', task.bucket)
      assert_equal('export/test.csv.zip', task.key)
      assert_equal('bigquery-public-data', task.project_id)
      assert_equal('samples', task.dataset_id)
      assert_equal('shakespeare', task.table_id)
      assert_equal('GZIP', task.compression)
      assert_equal(120, task.wait)
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
    assert_true(output.is_a? Tumugi::Plugin::GoogleCloudStorageFileTarget)
    assert_equal('gs://tumugi-plugin-bigquery/export/test.csv.zip', output.path)
  end

  test "#run" do
    task = @klass.new
    output = task.output
    task.run
    output.open("r") do |f|
      count = 0
      header = ''
      in_row = ''
      Zlib::GzipReader.open(f) do |gz|
        while s = gz.gets
          if count == 0
            header = s
          end
          count += 1
          if s.start_with?("in,")
            in_row = s
          end
        end
      end
      assert_equal(164657, count)
      assert_equal("word,word_count,corpus,corpus_date\n", header)
      assert_equal("in,255,kinghenryviii,1612\n", in_row)
    end
  end
end
