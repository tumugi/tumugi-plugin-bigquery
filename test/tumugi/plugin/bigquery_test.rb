require 'test_helper'

class Tumugi::Plugin::BigqueryTest < Test::Unit::TestCase
  def test_that_it_has_a_version_number
    refute_nil ::Tumugi::Plugin::Bigquery::VERSION
  end
end
