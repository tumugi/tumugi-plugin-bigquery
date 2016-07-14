require 'tumugi'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryQueryTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_query', self)

      param :query, type: :string, required: true
      param :project_id, type: :string
      param :dataset_id, type: :string, required: true
      param :table_id, type: :string, required: true
      param :mode, type: :string, default: 'truncate' # append, empty
      param :flatten_results, type: :bool, default: true
      param :use_legacy_sql, type: :bool, default: true

      param :wait, type: :int, default: 60

      def output
        @output ||= Tumugi::Plugin::BigqueryTableTarget.new(project_id: project_id, dataset_id: dataset_id, table_id: table_id)
      end

      def completed?
        if mode.to_sym == :append && !finished?
          false
        else
          super
        end
      end

      def run
        log "Launching Query"
        log "Query: #{query}"
        log "Query destination: #{output}"

        bq_client = output.client
        bq_client.query(query,
                        project_id: project_id,
                        dataset_id: output.dataset_id,
                        table_id: output.table_id,
                        mode: mode.to_sym,
                        flatten_results: flatten_results,
                        use_legacy_sql: use_legacy_sql,
                        wait: wait)
      end
    end
  end
end
