require_relative '../../test_helper'
require 'tumugi/plugin/task/bigquery_export'
require 'tumugi/plugin/target/google_cloud_storage_file'
require 'tumugi/plugin/target/local_file'

class Tumugi::Plugin::BigqueryExportTaskTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  setup do
    @klass = Class.new(Tumugi::Plugin::BigqueryExportTask)
    @klass.param_set :project_id, 'bigquery-public-data'
    @klass.param_set :job_project_id, 'tumugi-plugin-bigquery'
    @klass.param_set :dataset_id, 'samples'
    @klass.param_set :table_id, 'shakespeare'
    @klass.param_set :compression, 'GZIP'
  end

  sub_test_case "parameters" do
    test "should set correctly" do
      task = @klass.new
      assert_equal('bigquery-public-data', task.project_id)
      assert_equal('tumugi-plugin-bigquery', task.job_project_id)
      assert_equal('samples', task.dataset_id)
      assert_equal('shakespeare', task.table_id)
      assert_equal('CSV', task.destination_format)
      assert_equal('GZIP', task.compression)
      assert_equal(120, task.wait)
      assert_equal(10000, task.page_size)
    end

    data({
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

  test "export to Google Cloud Storage" do
    task = @klass.new
    task.instance_eval do
      def output
        Tumugi::Plugin::GoogleCloudStorageFileTarget.new(bucket: 'tumugi-plugin-bigquery', key: 'export/test.csv.zip')
      end
    end
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

  test "export CSV to local file" do
    task = @klass.new
    task.instance_eval do
      def output
        Tumugi::Plugin::LocalFileTarget.new('tmp/export.csv')
      end
    end
    output = task.output
    task.run
    output.open("r") do |f|
      count = 0
      header = ''
      in_row = ''
      while s = f.gets
        if count == 0
          header = s
        end
        count += 1
        if s.start_with?("in,")
          in_row = s
        end
      end
      assert_equal(164657, count)
      assert_equal("word,word_count,corpus,corpus_date\n", header)
      assert_equal("in,255,kinghenryviii,1612\n", in_row)
    end
  end

  test "export JSON to local file" do
    @klass.param_set :destination_format, 'NEWLINE_DELIMITED_JSON'
    task = @klass.new
    task.instance_eval do
      def output
        Tumugi::Plugin::LocalFileTarget.new('tmp/export.json')
      end
    end
    output = task.output
    task.run
    output.open("r") do |f|
      count = 0
      in_row = ''
      while s = f.gets
        json = JSON.parse(s)
        count += 1
        if json["word"] == "in"
          in_row = s
        end
      end
      assert_equal(164656, count)
      assert_equal("#{JSON.generate({"word" => "in", "word_count" => "255", "corpus" => "kinghenryviii", "corpus_date" => "1612"})}\n", in_row)
    end
  end

  test "raise error if output is not FileSystemTarget" do
    task = @klass.new
    task.instance_eval do
      def output
        Tumugi::Target.new
      end
    end
    assert_raise(Tumugi::TumugiError) do
      task.run
    end
  end

  test "raise error if format is AVRO and not to export Google Cloud Storage" do
    @klass.param_set :destination_format, 'AVRO'
    task = @klass.new
    task.instance_eval do
      def output
        Tumugi::Plugin::LocalFileTarget.new('tmp/export.csv')
      end
    end
    assert_raise(Tumugi::TumugiError) do
      task.run
    end
  end
end
