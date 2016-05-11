require 'kura'
require_relative './error'

Tumugi::Config.register_section('bigquery', :project_id, :client_email, :private_key)

module Tumugi
  module Plugin
    module Bigquery
      class Client
        attr_reader :project_id

        def initialize(project_id: nil, client_email: nil, private_key: nil)
          config = Tumugi.config.section('bigquery')
          @project_id = project_id || config.project_id
          @client_email = client_email || config.client_email
          @private_key = private_key || config.private_key
          @client = Kura.client(@project_id, @client_email, @private_key)
        rescue Kura::ApiError => e
          raise Tumugi::Plugin::Bigquery::BigqueryError.new(e.reason, e.message)
        end

        def dataset_exist?(dataset_id, project_id: @project_id)
          !@client.dataset(dataset_id, project_id: project_id).nil?
        rescue Kura::ApiError => e
          raise Tumugi::Plugin::Bigquery::BigqueryError.new(e.reason, e.message)
        end

        def table_exist?(dataset_id, table_id, project_id: @project_id)
          !@client.table(dataset_id, table_id, project_id: project_id).nil?
        rescue Kura::ApiError => e
          raise Tumugi::Plugin::Bigquery::BigqueryError.new(e.reason, e.message)
        end
      end
    end
  end
end
