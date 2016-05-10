require 'kura'

module Kura
  class Client
    # Monkey path for kura for get_job_query_results API
    # http://www.rubydoc.info/github/google/google-api-ruby-client/Google/Apis/BigqueryV2/BigqueryService#get_job_query_results-instance_method
    def job_query_results(job_id, project_id: @default_project_id, &blk)
      if blk
        @api.get_job_query_results(project_id, job_id) do |j, e|
          j.kura_api = self
          blk.call(j, e)
        end
      else
        @api.get_job_query_results(project_id, job_id).tap{|j| j.kura_api = self}
      end
    rescue
      process_error($!)
    end
  end
end
