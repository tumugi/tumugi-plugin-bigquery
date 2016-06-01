require 'tumugi'
require 'tumugi/plugin/target/google_cloud_storage_file'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryExportTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_export', self)

      param :bucket, type: :string, required: true
      param :key, type: :string, required: true
      param :project_id, type: :string
      param :dataset_id, type: :string, required: true
      param :table_id, type: :string, required: true

      param :compression, type: :string, default: 'NONE' # GZIP
      param :destination_format, type: :string, default: 'CSV' # NEWLINE_DELIMITED_JSON, AVRO

      # Only effected if destiation_format == 'CSV'
      param :field_delimiter, type: :string, default: ','
      param :print_header, type: :bool, default: true

      param :wait, type: :integer, default: 120

      def output
        Tumugi::Plugin::GoogleCloudStorageFileTarget.new(bucket: bucket, key: key)
      end

      def run
        proj_id = project_id || client.project_id
        table = Tumugi::Plugin::Bigquery::Table.new(project_id: proj_id, dataset_id: dataset_id, table_id: table_id)
        dest_uri = normalize_uri(key)

        log "Source: #{table}"
        log "Destination: #{output}"

        opts = {
          compression: compression,
          destination_format: destination_format,
          field_delimiter: field_delimiter,
          print_header: print_header,
          project_id: proj_id,
          job_project_id: client.project_id,
          wait: wait
        }
        client.extract(dataset_id, table_id, dest_uri, opts)
      end

      private

      def client
        @client ||= Tumugi::Plugin::Bigquery::Client.new
      end

      def normalize_path(path)
        unless path.start_with?('/')
          "/#{path}"
        else
          path
        end
      end

      def normalize_uri(path)
        "gs://#{bucket}#{normalize_path(path)}"
      end
    end
  end
end
