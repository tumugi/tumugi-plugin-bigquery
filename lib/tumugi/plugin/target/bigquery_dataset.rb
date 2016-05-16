require 'forwardable'
require 'tumugi'
require_relative '../bigquery/client'
require_relative '../bigquery/dataset'

module Tumugi
  module Plugin
    class BigqueryDatasetTarget
      Tumugi::Plugin.register_target('bigquery_dataset', self)

      attr_reader :project_id, :dataset_id, :client

      extend Forwardable
      def_delegators :@dataset, :dataset_name, :dataset_full_name, :to_s

      def initialize(project_id: nil, dataset_id:, client: nil)
        cfg = Tumugi.config.section('bigquery')
        @project_id = project_id || cfg.project_id
        @dataset_id = dataset_id
        @client = client || Tumugi::Plugin::Bigquery::Client.new(project_id: @project_id)
        @dataset = Tumugi::Plugin::Bigquery::Dataset.new(project_id: @project_id, dataset_id: @dataset_id)
      end

      def exist?
        @client.dataset_exist?(@dataset_id, project_id: @project_id)
      end
    end
  end
end
