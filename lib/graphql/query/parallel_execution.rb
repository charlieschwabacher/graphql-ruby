module GraphQL
  class Query
    class ParallelExecution < GraphQL::Query::BaseExecution
      attr_reader :counter
      def initialize
        Celluloid.boot unless Celluloid.running?
        @counter = 0
        @condition = Celluloid::Condition.new
      end

      def increment
        @counter += 1
      end

      def wait
        @condition.wait
      end

      def signal(arg = nil)
        @condition.signal(arg)
      end

      class OperationResolution < GraphQL::Query::SerialExecution::OperationResolution
        def result
          result_futures = super
          error = execution_strategy.wait
          if error
            raise(error)
          else
            get_data(result_futures)
          end
        end

        def get_data(futures_array)
          # it might be an already-finished value
          return futures_array if !futures_array.is_a?(Array)

          if futures_array.all? { |f| f.is_a?(Celluloid::Future) }
            futures_array.each_with_object({}) do |future, memo|
              value = future.value

              if value.is_a?(Array)
                value = get_data(value)
              end

              value.each do |key, array|
                if array.is_a?(Array)
                  value[key] = get_data(array)
                end
              end
              memo.merge!(value)
            end
          else
            futures_array.map { |i| get_data(i) }
          end
        end
      end

      class SelectionResolution < GraphQL::Query::SerialExecution::SelectionResolution
        def result
          selections.map do |ast_field|
            field_count = execution_strategy.increment
            Celluloid::Future.new do
              begin
                field_value = resolve_field(ast_field)
                if field_count == execution_strategy.counter
                  execution_strategy.signal
                end
                field_value
              rescue StandardError => e
                execution_strategy.signal(e)
              end
            end
          end
        end
      end

      class FieldResolution < GraphQL::Query::SerialExecution::FieldResolution
      end

      class InlineFragmentResolution < GraphQL::Query::SerialExecution::InlineFragmentResolution
      end

      class FragmentSpreadResolution < GraphQL::Query::SerialExecution::FragmentSpreadResolution
      end
    end
  end
end
