require 'forwardable'
require 'kura'
require_relative './extensions/kura/client'

Tumugi::Config.register_section('bigquery', :project_id, :client_email, :private_key)

module Tumugi
  module Plugin
    module Bigquery
      class Client
        extend Forwardable
        def_delegators :@client,
                        :batch, :projects,
                        :datasets, :dataset, :insert_dataset, :patch_dataset,
                        :tables, :table, :insert_table, :patch_table, :delete_table,
                        :list_tabledata, :insert_tabledata,
                        :insert_job, :query, :load, :extract, :copy,
                        :job, :cancel_job, :job_finished?, :wait_job,
                        :job_query_results

        attr_reader :project_id

        def initialize(project_id: nil, client_email: nil, private_key: nil)
          config = Tumugi.config.section('bigquery')
          @project_id = project_id || config.project_id
          @client_email = client_email || config.client_email
          @private_key = private_key || config.private_key
          @client = Kura.client(@project_id, @client_email, @private_key)
        end
      end
    end
  end
end
