require_relative '../../../test_helper'
require 'tumugi/plugin/bigquery/client'

class Tumugi::Plugin::Bigquery::ClientTest < Test::Unit::TestCase
  include Tumugi::Plugin::BigqueryTestHelper

  test "#initialize" do
    stub.proxy(Kura).client { |o| o }
    client = Tumugi::Plugin::Bigquery::Client.new(credential)
    assert_equal(credential[:project_id], client.project_id)
    assert_received(Kura) { |o| o.client(credential[:project_id], credential[:client_email], credential[:private_key]) }
  end
end
