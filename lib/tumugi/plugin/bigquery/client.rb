require 'kura'
require 'json'
require_relative './error'

Tumugi::Config.register_section('bigquery', :project_id, :client_email, :private_key, :private_key_file)

module Tumugi
  module Plugin
    module Bigquery
      class Client
        attr_reader :project_id

        def initialize(project_id: nil, client_email: nil, private_key: nil, private_key_file: nil)
          @project_id = project_id

          if client_email.nil? && private_key.nil?
            if private_key_file.nil?
              raise Tumugi::Plugin::Bigquery::BigqueryError.new("You must provide 'private_key_file'", "No authentication info")
            else
              @client = Kura.client(private_key_file)
              if @project_id.nil?
                key = JSON.parse(File.read(private_key_file))
                @project_id = key['project_id']
              end
            end
          else
            @client = Kura.client(@project_id, client_email, private_key)
          end
        rescue Kura::ApiError => e
          process_error(e)
        end

        def projects(limit: 1000, &blk)
          @client.projects(limit: limit, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def datasets(project_id: nil, all: false, limit: 1000, &blk)
          @client.datasets(project_id: project_id || @project_id, all: all, limit: limit, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def dataset(dataset_id, project_id: nil, &blk)
          @client.dataset(dataset_id, project_id: project_id || @project_id, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def dataset_exist?(dataset_id, project_id: nil)
          !@client.dataset(dataset_id, project_id: project_id || @project_id).nil?
        rescue Kura::ApiError => e
          process_error(e)
        end

        def insert_dataset(dataset_id, project_id: nil, &blk)
          @client.insert_dataset(dataset_id, project_id: project_id || @project_id, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def delete_dataset(dataset_id, project_id: nil, delete_contents: true, &blk)
          @client.delete_dataset(dataset_id, project_id: project_id || @project_id, delete_contents: delete_contents, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def patch_dataset(dataset_id, project_id: nil,
                          access: nil,
                          description: :na,
                          default_table_expiration_ms: :na,
                          friendly_name: :na,
                          &blk)
          @client.patch_dataset(dataset_id, project_id: project_id || @project_id,
                                access: access,
                                description: description,
                                default_table_expiration_ms: default_table_expiration_ms,
                                friendly_name: friendly_name,
                                &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def tables(dataset_id, project_id: nil, limit: 1000, &blk)
          @client.tables(dataset_id, project_id: project_id || @project_id, limit: limit, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def table_exist?(dataset_id, table_id, project_id: nil)
          !@client.table(dataset_id, table_id, project_id: project_id || @project_id).nil?
        rescue Kura::ApiError => e
          process_error(e)
        end

        def insert_table(dataset_id, table_id,
                          project_id: nil,
                          expiration_time: nil,
                          friendly_name: nil,
                          schema: nil,
                          description: nil,
                          query: nil,
                          external_data_configuration: nil,
                          &blk)
          @client.insert_table(dataset_id, table_id,
                                project_id: project_id || @project_id,
                                expiration_time: expiration_time,
                                friendly_name: friendly_name,
                                schema: schema,
                                description: description,
                                query: query,
                                external_data_configuration: external_data_configuration,
                                &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def delete_table(dataset_id, table_id, project_id: nil, &blk)
          @client.delete_table(dataset_id, table_id, project_id: project_id || @project_id, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def patch_table(dataset_id, table_id,
                        project_id: nil,
                        expiration_time: :na,
                        friendly_name: :na,
                        description: :na,
                        &blk)
          @client.patch_table(dataset_id, table_id,
                              project_id: project_id || @project_id,
                              expiration_time: expiration_time,
                              friendly_name: friendly_name,
                              description: description,
                              &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def list_tabledata(dataset_id, table_id, project_id: nil, start_index: 0, max_result: 100, page_token: nil, schema: nil, &blk)
          @client.list_tabledata(dataset_id, table_id,
                                  project_id: project_id || @project_id,
                                  start_index: start_index,
                                  max_result: max_result,
                                  page_token: page_token,
                                  schema: schema,
                                  &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def insert_tabledata(dataset_id, table_id, rows, project_id: nil, ignore_unknown_values: false, skip_invalid_rows: false, template_suffix: nil)
          @client.insert_tabledata(dataset_id, table_id, rows,
                                  project_id: project_id || @project_id,
                                  ignore_unknown_values: ignore_unknown_values,
                                  skip_invalid_rows: skip_invalid_rows,
                                  template_suffix: template_suffix)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def insert_job(configuration, job_id: nil, project_id: nil, media: nil, wait: nil, &blk)
          @client.insert_job(configuration, job_id: job_id, project_id: project_id || @project_id, media: media, wait: wait, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def query(sql, mode: :truncate,
                  dataset_id: nil, table_id: nil,
                  allow_large_results: true,
                  flatten_results: true,
                  priority: "INTERACTIVE",
                  use_query_cache: true,
                  user_defined_function_resources: nil,
                  project_id: nil,
                  job_project_id: nil,
                  job_id: nil,
                  wait: nil,
                  dry_run: false,
                  &blk)
          @client.query(sql, mode: mode,
                        dataset_id: dataset_id, table_id: table_id,
                        allow_large_results: allow_large_results,
                        flatten_results: flatten_results,
                        priority: priority,
                        use_query_cache: use_query_cache,
                        user_defined_function_resources: user_defined_function_resources,
                        project_id: project_id || @project_id,
                        job_project_id: job_project_id || @project_id,
                        job_id: job_id,
                        wait: wait,
                        dry_run: dry_run,
                        &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def load(dataset_id, table_id, source_uris=nil,
                 schema: nil,
                 field_delimiter: ",",
                 mode: :append,
                 allow_jagged_rows: false,
                 max_bad_records: 0,
                 ignore_unknown_values: false,
                 allow_quoted_newlines: false,
                 quote: '"',
                 skip_leading_rows: 0,
                 source_format: "CSV",
                 project_id: nil,
                 job_project_id: nil,
                 job_id: nil,
                 file: nil, wait: nil,
                 dry_run: false,
                 &blk)
          @client.load(dataset_id, table_id, source_uris=source_uris,
                       schema: schema,
                       field_delimiter: field_delimiter,
                       mode: mode,
                       allow_jagged_rows: allow_jagged_rows,
                       max_bad_records: max_bad_records,
                       ignore_unknown_values: ignore_unknown_values,
                       allow_quoted_newlines: allow_quoted_newlines,
                       quote: quote,
                       skip_leading_rows: skip_leading_rows,
                       source_format: source_format,
                       project_id: project_id || @project_id,
                       job_project_id: job_project_id || @project_id,
                       job_id: job_id,
                       file: file,
                       wait: wait,
                       dry_run: dry_run,
                       &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def extract(dataset_id, table_id, dest_uris,
                    compression: "NONE",
                    destination_format: "CSV",
                    field_delimiter: ",",
                    print_header: true,
                    project_id: nil,
                    job_project_id: nil,
                    job_id: nil,
                    wait: nil,
                    dry_run: false,
                    &blk)
          @client.extract(dataset_id, table_id, dest_uris,
                          compression: compression,
                          destination_format: destination_format,
                          field_delimiter: field_delimiter,
                          print_header: print_header,
                          project_id: project_id || @project_id,
                          job_project_id: job_project_id || @project_id,
                          job_id: job_id,
                          wait: wait,
                          dry_run: dry_run,
                          &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def copy(src_dataset_id, src_table_id, dest_dataset_id, dest_table_id,
                 mode: :truncate,
                 src_project_id: nil,
                 dest_project_id: nil,
                 job_project_id: dest_project_id,
                 job_id: nil,
                 wait: nil,
                 dry_run: false,
                 &blk)
          @client.copy(src_dataset_id, src_table_id, dest_dataset_id, dest_table_id,
                       mode: mode,
                       src_project_id: src_project_id || @project_id,
                       dest_project_id: dest_project_id || @project_id,
                       job_project_id: job_project_id || @project_id,
                       job_id: job_id,
                       wait: wait,
                       dry_run: dry_run,
                       &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def job(job_id, project_id: nil, &blk)
          @client.job(job_id, project_id: project_id || @project_id, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def cancel_job(job_id, project_id: nil, &blk)
          @client.cancel_job(job_id, project_id: project_id || @project_id, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        def wait_job(job, timeout=60*10, project_id: nil, &blk)
          @client.wait_job(job, timeout, project_id: project_id || @project_id, &blk)
        rescue Kura::ApiError => e
          process_error(e)
        end

        private

        def process_error(e)
          raise Tumugi::Plugin::Bigquery::BigqueryError.new(e.reason, e.message)
        end
      end
    end
  end
end
