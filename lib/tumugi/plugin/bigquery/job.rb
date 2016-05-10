module Tumugi
  module Plugin
    module Bigquery
      class Job
        attr_reader :id

        def initialize(client, job_id)
          @client = client
          @id = job_id
        end

        def result_size
          @result_size ||= Integer(result.total_rows)
        end

        def schema
          @schema ||= result.schema
        end

        def result
          @result ||= @client.job_query_results(@id)
        end
      end
    end
  end
end
