require 'tumugi'
require_relative '../target/bigquery_dataset'

module Tumugi
  module Plugin
    class BigqueryDatasetTask < Tumugi::Task
      Tumugi::Plugin.register_task('bigquery_dataset', self)

      param :project, type: :string
      param :dataset, type: :string, required: true

      def output
        Tumugi::Plugin::BigqueryDatasetTarget.new(project_id: project, dataset_id: dataset)
      end

      def run
        log "Dataset: #{output}"
        bq_client = output.client
        bq_client.insert_dataset(dataset, project_id: project)
      end
    end
  end
end
