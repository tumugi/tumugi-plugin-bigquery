module Tumugi
  module Plugin
    module Bigquery
      class Dataset
        attr_reader :project_id, :dataset_id

        def initialize(project_id:, dataset_id:)
          @project_id = project_id
          @dataset_id = dataset_id
        end

        def dataset_name
          "#{dataset_id}"
        end

        def dataset_full_name
          "#{project_id}:#{dataset_id}"
        end

        def to_s
          "bq://#{project_id}/#{dataset_id}"
        end
      end
    end
  end
end
