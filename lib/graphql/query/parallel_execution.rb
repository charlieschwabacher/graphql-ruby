module GraphQL
  class Query
    # Utilize Celluloid::Future to run field resolution in parallel.
    #
    # Basically the approach is:
    #  - As you start to resolve a field, increment the total field counter
    #  - When a field ends, if it was the last one, trigger to collect the whole response
    #  - On error, spit the error out and raise it like normal
    class ParallelExecution < GraphQL::Query::BaseExecution
      attr_reader :total_field_counter
      def initialize
        Celluloid.boot unless Celluloid.running?
        @total_field_counter = 0
        @condition = Celluloid::Condition.new
      end

      def increment
        @total_field_counter += 1
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
            finish_all_futures(result_futures)
          end
        end

        # Recurse over `result_object`, finding any futures and
        # getting their finished values.
        def finish_all_futures(result_object)
          # It's already-finished value
          return result_object if !result_object.is_a?(Array)

          if result_object.all? { |f| f.is_a?(Celluloid::Future) }
            # It's the result of a selection set
            result_object.each_with_object({}) do |future, memo|
              resolved_value = future.value

              if resolved_value.is_a?(Array)
                resolved_value = finish_all_futures(resolved_value)
              end

              resolved_value.each do |key, value|
                resolved_value[key] = finish_all_futures(value)
              end
              memo.merge!(resolved_value)
            end
          else
            # It's the result of a list field
            result_object.map { |item| finish_all_futures(item) }
          end
        end
      end

      class SelectionResolution < GraphQL::Query::SerialExecution::SelectionResolution
        # For each field, tell the execution strategy to wait for one more,
        # Then start async resolution.
        #
        # After the resolution, if that field is the last one,
        # tell the execution strategy that we're finished, it should clean up data now.
        #
        # Handle errors by sending them to the execution strategy.
        # @return [Array<Celluloid::Future>] Futures for the field resolve values
        def result
          selections.map do |ast_field|
            field_idx = execution_strategy.increment
            Celluloid::Future.new do
              begin
                field_value = resolve_field(ast_field)
                if field_idx == execution_strategy.total_field_counter
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
