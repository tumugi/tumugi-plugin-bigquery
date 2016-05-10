module Tumugi
  module Plugin
    module Bigquery
      class ResultProxy
        def initialize(cllient, job)
          @client = client
          @job = job
        end

        def job_id
          @job.id
        end

        def size
          @job.result_size
        end

        def schema
          @job.schema
        end

        def each(&block)
          _rows.each(&block)
        end

        private

        def _columns
          schema.fields.map do |field|
            field.name
          end
          # TODO: Suppore nested schema if the column type is RECORD
        end

        def _rows
          @job.result.rows.map do |row|
            r = {}
            cells = row.f
            _columns.each_with_index do |c, i|
              r[c] = cells[i].v
            end
            r
          end
        end
      end
    end
  end
end
