require_relative './test_helper'
require 'tumugi/cli'

class Tumugi::Plugin::Bigquery::CLITest < Test::Unit::TestCase
  examples = {
    'copy' => ['copy.rb', 'task1'],
    'dataset' => ['dataset.rb', 'task1'],
    'query' => ['query.rb', 'task1'],
  }

  def invoke(file, task, options)
    Tumugi::CLI.new.invoke(:run_, [task], options.merge(file: "./examples/#{file}", quiet: true))
  end

  data(examples)
  test 'success' do |(file, task)|
    assert_true(invoke(file, task, worker: 4, config: "./examples/tumugi_config_example.rb"))
  end
end
