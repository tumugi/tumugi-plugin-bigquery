require 'fileuitls'
require 'json'
require 'pathname'
require 'tumugi'
require_relative '../bigquery/client'

module Tumugi
  module Plugin
    class BigqueryJobResultTarget
      Tumugi::Plugin.register_target('bigquery_job_result', self)

      def initialize(project_id: nil, path:, client: nil)
        cfg = Tumugi.config.section('bigquery')
        @project_id = project_id || cfg.project_id
        @path = path
        @client = client || Tumugi::Plugin::Bigquery::Client.new(project_id: @project_id)
      end

      def save_result_state(result)
        state_dir = Pathname.new(@path).parent
        if state_dir != '' && !File.exist?(state_dir)
          FileUtils.mkdir_p(state_dir)
        end

        File.open(@path, 'w') do |f|
          f.print JSON.generate("job_id" => result.job_id)
        end
      end

      def load_result_state
        JSON.parse(File.read(@path))
      end

      def job_id
        load_result_state["job_id"]
      end

      def result
        return @result if @result
        job = Tumugi::Plugin::Bigquery::Job.new(job_id)
        @result = Tumugi::Plugin::Bigquery::ResultProxy.new(job)
      end

      def exist?
        if File.exist?(@path)
          job = @client.job(job_id, project_id: @project_id)
          @client.job_finished?(job)
        else
          false
        end
      end

      def to_s
        "#{@project_id}:#{job_id}"
      end
    end
  end
end
