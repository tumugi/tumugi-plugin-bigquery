require 'tumugi'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryQueryTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_query', self)

      param :query, type: :string, required: true
      param :project, type: :string
      param :dataset, type: :string, required: true
      param :table, type: :string, require: true
      param :wait, type: :int, deafult: 60

      def output
        Tumugi::Plugin::BigqueryTableTarget.new(project_id: project, dataset_id: dataset, table_id: table)
      end

      def run
        log "Launching Query"
        log "Query: #{query}"
        log "Query destination: #{output}"

        sleep 5

        bq_client = output.client
        bq_client.query(query, project_id: project, dataset_id: output.dataset_id, table_id: output.table_id, wait: wait)
      end
    end
  end
end
