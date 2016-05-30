require 'tumugi'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryLoadTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_load', self)

      param :bucket, type: :string, required: true
      param :key, type: :string, required: true
      param :project_id, type: :string
      param :dataset_id, type: :string, required: true
      param :table_id, type: :string, required: true

      param :schema # type: :array
      param :field_delimiter, type: :string, default: ','
      param :mode, type: :string, default: 'append' # truncate, empty
      param :allow_jagged_rows, type: :bool, default: false
      param :max_bad_records, type: :integer, default: 0
      param :ignore_unknown_values, type: :bool, default: false
      param :allow_quoted_newlines, type: :bool, default: false
      param :quote, type: :string, default: '"'
      param :skip_leading_rows, type: :interger, default: 0
      param :source_format, type: :string, default: 'CSV' # NEWLINE_DELIMITED_JSON, AVRO
      param :wait, type: :integer, default: 60

      def output
        opts = { dataset_id: dataset_id, table_id: table_id }
        opts[:project_id] = project_id if project_id
        Tumugi::Plugin::BigqueryTableTarget.new(opts)
      end

      def run
        if mode != 'append'
          raise Tumugi::ParameterError.new("Parameter 'schema' is required when 'mode' is 'truncate' or 'empty'") if schema.nil?
        end

        object_id = key
        unless object_id.start_with?('/')
          object_id = "/#{key}"
        end
        source_uri = "gs://#{bucket}#{object_id}"
        log "Source: #{source_uri}"
        log "Destination: #{output}"

        bq_client = output.client
        opts = {
          schema: schema,
          field_delimiter: field_delimiter,
          mode: mode.to_sym,
          allow_jagged_rows: allow_jagged_rows,
          max_bad_records: max_bad_records,
          ignore_unknown_values: ignore_unknown_values,
          allow_quoted_newlines: allow_quoted_newlines,
          quote: quote,
          skip_leading_rows: skip_leading_rows,
          source_format: source_format,
          project_id: _output.project_id,
          wait: wait
        }
        bq_client.load(_output.dataset_id, _output.table_id, source_uri, opts)
      end
    end
  end
end
