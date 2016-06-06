require 'tumugi'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryCopyTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_copy', self)

      param :src_project_id, type: :string
      param :src_dataset_id, type: :string, required: true
      param :src_table_id, type: :string, required: true
      param :dest_project_id, type: :string
      param :dest_dataset_id, type: :string, required: true
      param :dest_table_id, type: :string, required: true
      param :wait, type: :int, default: 60

      def output
        return @output if @output
        
        opts = { dataset_id: dest_dataset_id, table_id: dest_table_id }
        opts[:project_id] = dest_project_id if dest_project_id
        @output = Tumugi::Plugin::BigqueryTableTarget.new(opts)
      end

      def run
        log "Source: bq://#{src_project_id}/#{src_dataset_id}/#{src_table_id}"
        log "Destination: #{output}"

        bq_client = output.client
        opts = { wait: wait }
        opts[:src_project_id] = src_project_id if src_project_id
        opts[:dest_project_id] = dest_project_id if dest_project_id
        bq_client.copy(src_dataset_id, src_table_id, dest_dataset_id, dest_table_id, opts)
      end
    end
  end
end
