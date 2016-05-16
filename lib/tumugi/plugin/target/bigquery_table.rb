require 'forwardable'
require 'tumugi'
require_relative '../bigquery/client'
require_relative '../bigquery/table'

module Tumugi
  module Plugin
    class BigqueryTableTarget
      Tumugi::Plugin.register_target('bigquery_table', self)

      extend Forwardable
      def_delegators :@table, :table_name, :table_full_name, :to_s

      attr_reader :project_id, :dataset_id, :table_id, :client

      def initialize(project_id: nil, dataset_id:, table_id:, client: nil)
        cfg = Tumugi.config.section('bigquery')
        @project_id = project_id || cfg.project_id
        @dataset_id = dataset_id
        @table_id = table_id
        @client = client || Tumugi::Plugin::Bigquery::Client.new(project_id: @project_id)
        @table = Tumugi::Plugin::Bigquery::Table.new(project_id: @project_id, dataset_id: @dataset_id, table_id: @table_id)
      end

      def exist?
        @client.table_exist?(@dataset_id, @table_id, project_id: @project_id)
      end
    end
  end
end
