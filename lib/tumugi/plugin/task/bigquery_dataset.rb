require 'tumugi'
require_relative '../target/bigquery_dataset'

module Tumugi
  module Plugin
    class BigqueryDatasetTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_dataset', self)

      param :project_id, type: :string
      param :dataset_id, type: :string, required: true

      def output
        Tumugi::Plugin::BigqueryDatasetTarget.new(project_id: project_id, dataset_id: dataset_id)
      end

      def run
        log "Dataset: #{output}"
        bq_client = output.client
        bq_client.insert_dataset(dataset_id)
      end
    end
  end
end
