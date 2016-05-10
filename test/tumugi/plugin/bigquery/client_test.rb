require_relative '../../../test_helper'
require 'json'
require 'tumugi/plugin/bigquery/client'

class Tumugi::Plugin::Bigquery::ClientTest < Test::Unit::TestCase
  TEST_DATASETS = [ "test1_#{Time.now.to_i}", "test2_#{Time.now.to_i}" ]

  class << self
    def startup
      @kura = Kura.client(credential[:project_id], credential[:client_email], credential[:private_key])
      TEST_DATASETS.each { |dataset| @kura.insert_dataset(dataset) }
    end

    def shutdown
      TEST_DATASETS.each { |dataset| @kura.delete_dataset(dataset, delete_contents: true) }
    end
  end

  setup do
    @client = Tumugi::Plugin::Bigquery::Client.new(credential)
  end

  test "can connect to BigQuery" do
    datasets = @client.datasets
    assert_equal(TEST_DATASETS.count, datasets.count)
    TEST_DATASETS.each_with_index do |dataset, i|
      assert_equal("#{@client.project_id}:#{dataset}", datasets[i].id)
    end
  end
end
