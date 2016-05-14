module Tumugi
  module Plugin
    module Bigquery
      class BigqueryError < Tumugi::TumugiError
        def initialize(reason, message)
          @reason = reason
          super(message)
        end
        attr_reader :reason
      end
    end
  end
end
