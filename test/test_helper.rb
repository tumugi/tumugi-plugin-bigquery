require 'coveralls'
Coveralls.wear!

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require 'test/unit'
require 'test/unit/rr'

require 'tumugi'
require 'kura'

module Tumugi
  module Plugin
    module BigqueryTestHelper
      TEST_DATASETS = [ "test1_#{SecureRandom.hex(10)}", "test2_#{SecureRandom.hex(10)}" ]

      def self.included(mod)
        mod.extend(ClassMethods)
      end

      module ClassMethods
        def startup
          return unless self.name.end_with?('Test') # ignore sub_test_case
          @kura = Kura.client(credential[:project_id], credential[:client_email], credential[:private_key])
          TEST_DATASETS.each do |dataset|
            @kura.insert_dataset(dataset)
            @kura.insert_table(dataset, 'test')
          end
        end

        def shutdown
          return unless self.name.end_with?('Test') # ignore sub_test_case
          TEST_DATASETS.each { |dataset| @kura.delete_dataset(dataset, delete_contents: true) }
          begin
            sleep 1
          end if @kura.datasets
        end
      end
    end
  end
end

def credential
  pkey = ENV['PRIVATE_KEY'].gsub(/\\n/, "\n")
  {
    project_id: ENV['PROJECT_ID'],
    client_email: ENV['CLIENT_EMAIL'],
    private_key: pkey
  }
end

Tumugi.config do |c|
  c.section('bigquery') do |s|
    s.project_id = credential[:project_id]
    s.client_email = credential[:client_email]
    s.private_key = credential[:private_key]
  end
end
