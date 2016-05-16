require_relative '../test_helper'
require 'tumugi/plugin/bigquery/version'

class Tumugi::Plugin::BigqueryTest < Test::Unit::TestCase
  test "has a version number" do
    refute_nil ::Tumugi::Plugin::Bigquery::VERSION
  end
end
