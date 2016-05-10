require 'tumugi'
require_relative '../bigquery/client'

module Tumugi
  module Plugin
    class BigqueryDatasetTarget
      Tumugi::Plugin.register_target('bigquery_dataset', self)

      def initialize(project_id: nil, dataset_id:, client: nil)
        cfg = Tumugi.config.section('bigquery')
        @project_id = project_id || cfg.project_id
        @dataset_id = dataset_id
        @client = client || Tumugi::Plugin::Bigquery::Client.new(project_id: @project_id)
      end

      def exist?
        @client.dataset(@dataset_id, project_id: @project_id)
      end

      def to_s
        "bq://#{@project_id}/#{@dataset_id}"
      end
    end
  end
end
