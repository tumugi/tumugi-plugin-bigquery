require_relative '../../test_helper'
require 'tumugi/plugin/bigquery/client'

class Tumugi::Plugin::Bigquery::ClientTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  setup do
    @client = Tumugi::Plugin::Bigquery::Client.new(credential)
  end

  test "#initialize" do
    stub.proxy(Kura).client { |o| o }
    client = Tumugi::Plugin::Bigquery::Client.new(credential)
    assert_equal(credential[:project_id], client.project_id)
    assert_received(Kura) { |o| o.client(credential[:project_id], credential[:client_email], credential[:private_key]) }
  end

  test "#projects" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).projects(limit: 1000) {}
    end
    @client.projects
  end

  test "#datasets" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).datasets(project_id: credential[:project_id], all: false, limit: 1000) {}
    end
    @client.datasets
  end

  test "#dataset" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).dataset(TEST_DATASETS[0], project_id: credential[:project_id]) {}
    end
    @client.dataset(TEST_DATASETS[0])
  end

  test "#dataset_exist?" do
    assert_true(@client.dataset_exist?(TEST_DATASETS[0]))
    assert_false(@client.dataset_exist?('not_found'))
  end

  test "#insert_dataset" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).insert_dataset(TEST_DATASETS[0], project_id: credential[:project_id]) {}
    end
    @client.insert_dataset(TEST_DATASETS[0])
  end

  test "#delete_dataset" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).delete_dataset(TEST_DATASETS[0], project_id: credential[:project_id], delete_contents: true) {}
    end
    @client.delete_dataset(TEST_DATASETS[0])
  end

  test "#patch_dataset" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).patch_dataset(TEST_DATASETS[0],
                                project_id: credential[:project_id],
                                access: nil,
                                description: :na,
                                default_table_expiration_ms: :na,
                                friendly_name: :na) {}
    end
    @client.patch_dataset(TEST_DATASETS[0])
  end

  test "#tables" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).tables(TEST_DATASETS[0], project_id: credential[:project_id], limit: 1000) {}
    end
    @client.tables(TEST_DATASETS[0])
  end

  test "#table_exist?" do
    assert_true(@client.table_exist?(TEST_DATASETS[0], 'test'))
    assert_false(@client.table_exist?(TEST_DATASETS[0], 'not_found'))
    assert_false(@client.table_exist?('not_found', 'not_found'))
  end

  test "#insert_table" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).insert_table(TEST_DATASETS[0], 'test',
                                project_id: credential[:project_id],
                                expiration_time: nil,
                                friendly_name: nil,
                                schema: nil,
                                description: nil,
                                query: nil,
                                external_data_configuration: nil) {}
    end
    @client.insert_table(TEST_DATASETS[0], 'test')
  end

  test "#delete_table" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).delete_table(TEST_DATASETS[0], 'test', project_id: credential[:project_id]) {}
    end
    @client.delete_table(TEST_DATASETS[0], 'test')
  end

  test "#patch_table" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).patch_table(TEST_DATASETS[0], 'test',
                              project_id: credential[:project_id],
                              expiration_time: :na,
                              friendly_name: :na,
                              description: :na) {}
    end
    @client.patch_table(TEST_DATASETS[0], 'test')
  end

  test "#list_tabledata" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).list_tabledata(TEST_DATASETS[0], 'test',
                              project_id: credential[:project_id],
                              start_index: 0, max_result: 100, page_token: nil, schema: nil) {}
    end
    @client.list_tabledata(TEST_DATASETS[0], 'test')
  end

  test "#insert_tabledata" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).insert_tabledata(TEST_DATASETS[0], 'test', [{key: 'val'}],
                              project_id: credential[:project_id],
                              ignore_unknown_values: false,
                              skip_invalid_rows: false,
                              template_suffix: nil) {}
    end
    @client.insert_tabledata(TEST_DATASETS[0], 'test', [{key: 'val'}])
  end

  test "#insert_job" do
    configuration = Google::Apis::BigqueryV2::JobConfiguration.new
    any_instance_of(Kura::Client) do |klass|
      mock(klass).insert_job(configuration,
                              job_id: nil, project_id: credential[:project_id],
                              media: nil, wait: nil) {}
    end
    @client.insert_job(configuration)
  end

  test "#query" do
    sql = "SELECT COUNT(*) AS cnt FROM [bigquery-public-data:samples.wikipedia]"
    any_instance_of(Kura::Client) do |klass|
      mock(klass).query(sql,
                        mode: :truncate,
                        dataset_id: nil, table_id: nil,
                        allow_large_results: true,
                        flatten_results: true,
                        priority: "INTERACTIVE",
                        use_query_cache: true,
                        user_defined_function_resources: nil,
                        project_id: credential[:project_id],
                        job_project_id: credential[:project_id],
                        job_id: nil,
                        wait: nil,
                        dry_run: false) {}
    end
    @client.query(sql)
  end

  test "#load" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).load(TEST_DATASETS[0], 'test', 'gs://tumugi-plugin-bigquery/test.csv',
                        schema: nil, delimiter: ",", field_delimiter: ",", mode: :append,
                        allow_jagged_rows: false, max_bad_records: 0,
                        ignore_unknown_values: false,
                        allow_quoted_newlines: false,
                        quote: '"', skip_leading_rows: 0,
                        source_format: "CSV",
                        project_id: credential[:project_id],
                        job_project_id: credential[:project_id],
                        job_id: nil,
                        file: nil, wait: nil,
                        dry_run: false) {}
    end
    @client.load(TEST_DATASETS[0], 'test', 'gs://tumugi-plugin-bigquery/test.csv')
  end

  test "#extract" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).extract(TEST_DATASETS[0], 'test', 'gs://tumugi-plugin-bigquery/test.csv',
                          compression: "NONE",
                          destination_format: "CSV",
                          field_delimiter: ",",
                          print_header: true,
                          project_id: credential[:project_id],
                          job_project_id: credential[:project_id],
                          job_id: nil,
                          wait: nil,
                          dry_run: false) {}
    end
    @client.extract(TEST_DATASETS[0], 'test', 'gs://tumugi-plugin-bigquery/test.csv')
  end

  test "#copy" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).copy('src_dataset', 'src_table', 'dst_dataset', 'dst_table',
                        mode: :truncate,
                        src_project_id: credential[:project_id],
                        dest_project_id: credential[:project_id],
                        job_project_id: credential[:project_id],
                        job_id: nil,
                        wait: nil,
                        dry_run: false) {}
    end
    @client.copy('src_dataset', 'src_table', 'dst_dataset', 'dst_table')
  end

  test "#job" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).job('test', project_id: credential[:project_id]) {}
    end
    @client.job('test')
  end

  test "#cancel_job" do
    any_instance_of(Kura::Client) do |klass|
      mock(klass).cancel_job('test', project_id: credential[:project_id]) {}
    end
    @client.cancel_job('test')
  end

  test "#wait_job" do
    job = Google::Apis::BigqueryV2::Job.new(id: 'test')
    any_instance_of(Kura::Client) do |klass|
      mock(klass).wait_job(job, 600, project_id: credential[:project_id]) {}
    end
    @client.wait_job(job)
  end
end
