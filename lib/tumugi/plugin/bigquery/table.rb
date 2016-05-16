require_relative './dataset'

module Tumugi
  module Plugin
    module Bigquery
      class Table
        attr_reader :project_id, :dataset_id, :table_id

        def initialize(project_id:, dataset_id:, table_id:)
          @project_id = project_id
          @dataset_id = dataset_id
          @table_id = table_id
        end

        def dataset
          Tumugi::Plugin::Bigquery::Dataset.new(project_id: @project_id, dataset_id: @dataset_id)
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
end
