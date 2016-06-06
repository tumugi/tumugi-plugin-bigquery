require 'json'
require 'tumugi'
require 'tumugi/plugin/file_system_target'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryExportTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_export', self)

      param :project_id, type: :string
      param :job_project_id, type: :string
      param :dataset_id, type: :string, required: true
      param :table_id, type: :string, required: true

      param :compression, type: :string, default: 'NONE' # GZIP
      param :destination_format, type: :string, default: 'CSV' # NEWLINE_DELIMITED_JSON, AVRO

      # Only effected if destiation_format == 'CSV'
      param :field_delimiter, type: :string, default: ','
      param :print_header, type: :bool, default: true

      param :page_size, type: :integer, default: 10000

      param :wait, type: :integer, default: 120

      def run
        unless output.is_a?(Tumugi::Plugin::FileSystemTarget)
          raise Tumugi::TumugiError.new("BigqueryExportTask#output must be return a instance of Tumugi::Plugin::FileSystemTarget")
        end

        client = Tumugi::Plugin::Bigquery::Client.new(config)
        table = Tumugi::Plugin::Bigquery::Table.new(project_id: client.project_id, dataset_id: dataset_id, table_id: table_id)
        job_project_id = client.project_id if job_project_id.nil?

        log "Source: #{table}"
        log "Destination: #{output}"

        if is_gcs?(output)
          export_to_gcs(client)
        else
          if destination_format.upcase == 'AVRO'
            raise Tumugi::TumugiError.new("destination_format='AVRO' is only supported when export to Google Cloud Storage")
          end
          if compression.upcase == 'GZIP'
            logger.warn("compression parameter is ignored, it's only supported when export to Google Cloud Storage")
          end
          export_to_file_system(client)
        end
      end

      private

      def is_gcs?(target)
        not target.to_s.match(/^gs:\/\/[^\/]+\/.+$/).nil?
      end

      def export_to_gcs(client)
        options = {
          compression: compression.upcase,
          destination_format: destination_format.upcase,
          field_delimiter: field_delimiter,
          print_header: print_header,
          project_id: client.project_id,
          job_project_id: job_project_id || client.project_id,
          wait: wait
        }
        client.extract(dataset_id, table_id, output.to_s, options)
      end

      def export_to_file_system(client)
        schema ||= client.table(dataset_id, table_id, project_id: client.project_id).schema.fields
        field_names = schema.map{|f| f.respond_to?(:[]) ? (f["name"] || f[:name]) : f.name }
        start_index = 0
        page_token = nil
        options = {
          max_result: page_size,
          project_id: client.project_id,
        }

        output.open('w') do |file|
          file.puts field_names.join(field_delimiter) if destination_format == 'CSV' && print_header
          begin
            table_data_list = client.list_tabledata(dataset_id, table_id, options.merge(start_index: start_index, page_token: page_token))
            start_index += page_size
            page_token = table_data_list[:next_token]
            table_data_list[:rows].each do |row|
              file.puts line(field_names, row, destination_format)
            end
          end while not page_token.nil?
        end
      end

      def line(field_names, row, format)
        case format
        when 'CSV'
          row.map{|v| v[1]}.join(field_delimiter)
        when 'NEWLINE_DELIMITED_JSON'
          JSON.generate(row.to_h)
        end
      end

      def config
        cfg = Tumugi.config.section('bigquery').to_h
        unless project_id.nil?
          cfg[:project_id] = project_id
        end
        cfg
      end
    end
  end
end
