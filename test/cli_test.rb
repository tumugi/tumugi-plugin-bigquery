require_relative './test_helper'

class Tumugi::Plugin::Bigquery::CLITest < Test::Unit::TestCase
  examples = {
    'copy' => ['copy.rb', 'task1'],
    'dataset' => ['dataset.rb', 'task1'],
    'query' => ['query.rb', 'task1'],
  }

  def exec(file, task, options)
    system("bundle exec tumugi run -f ./examples/#{file} #{options} #{task}")
  end

  data(examples)
  test 'success' do |(file, task)|
    assert_true(exec(file, task, "-w 4 --quiet -c ./examples/tumugi_config_example.rb"))
  end
end
