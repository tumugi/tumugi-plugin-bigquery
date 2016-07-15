require_relative './test_helper'
require 'tumugi/cli'

class Tumugi::Plugin::Bigquery::CLITest < Tumugi::Test::TumugiTestCase
  examples = {
    'copy' => ['copy.rb', 'task1'],
    'dataset' => ['dataset.rb', 'task1'],
    'export' => ['export.rb', 'task1'],
    'force_copy' => ['force_copy.rb', 'task1'],
    'query' => ['query.rb', 'task1'],
    'query_append' => ['query_append.rb', 'task1'],
  }

  setup do
    system('rm -rf tmp/*')
  end

  data do
    data_set = {}
    examples.each do |k, v|
      [1, 2, 8].each do |n|
        data_set["#{k}_workers_#{n}"] = (v.dup << n)
      end
    end
    data_set
  end
  test 'success' do |(file, task, worker)|
    assert_run_success("examples/#{file}", task, workers: worker, config: "./examples/tumugi_config_example.rb")
  end
end
