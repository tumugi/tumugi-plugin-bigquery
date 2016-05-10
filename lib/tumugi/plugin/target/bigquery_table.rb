require 'tumugi'
require_relative '../bigquery/client'

module Tumugi
  module Plugin
    class BigqueryTableTarget
      Tumugi::Plugin.register_target('bigquery_table', self)

      attr_reader :project_id, :dataset_id, :table_id, :client

      def initialize(project_id: nil, dataset_id:, table_id:, client: nil)
        cfg = Tumugi.config.section('bigquery')
        @project_id = project_id || cfg.project_id
        @dataset_id = dataset_id
        @table_id = table_id
        @client = client || Tumugi::Plugin::Bigquery::Client.new(project_id: @project_id)
      end

      def exist?
        @client.table(@dataset_id, @table_id, project_id: @project_id)
      end

      def table_name
        "#{dataset_id}.#{table_id}"
      end

      def table_full_name
        "#{project_id}:#{dataset_id}.#{table_id}"
      end

      def to_s
        "bq://#{project_id}/#{dataset_id}/#{table_id}"
      end
    end
  end
end
