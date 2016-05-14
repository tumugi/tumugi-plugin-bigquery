require 'tumugi'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryCopyTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_copy', self)

      param :src_project, type: :string
      param :src_dataset, type: :string, required: true
      param :src_table, type: :string, required: true
      param :dest_project, type: :string
      param :dest_dataset, type: :string, required: true
      param :dest_table, type: :string, required: true
      param :wait, type: :int, default: 60

      def output
        opts = { dataset_id: dest_dataset, table_id: dest_table }
        opts[:project_id] = dest_project if dest_project
        Tumugi::Plugin::BigqueryTableTarget.new(opts)
      end

      def run
        log "Source: bq://#{src_project}/#{src_dataset}/#{src_table}"
        log "Destination: #{output}"

        bq_client = output.client
        opts = { wait: wait }
        opts[:src_project_id] = src_project if src_project
        opts[:dest_project_id] = dest_project if dest_project
        bq_client.copy(src_dataset, src_table, dest_dataset, dest_table, opts)
      end
    end
  end
end
