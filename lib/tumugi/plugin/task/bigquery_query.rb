require 'tumugi'

require_relative '../bigquery/client'
require_relative '../target/bigquery_table'

module Tumugi
  module Plugin
    class BigqueryQueryTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_query', self)

      param :query, type: :string, required: true

      def run
        log "Launching Query"
        log "Query: #{query}"
        log "Query destination: #{output}"

        bq_client = output.client
        bq_client.query(query, dataset_id: output.dataset_id, table_id: output.table_id, wait: 120)
      end
    end
  end
end
