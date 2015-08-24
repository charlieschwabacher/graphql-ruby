module GraphQL
  class Query
    # Utilize Celluloid::Future to run field resolution in parallel.
    class ParallelExecution < GraphQL::Query::BaseExecution
      attr_reader :total_field_counter
      def initialize
        Celluloid.boot unless Celluloid.running?
        @pool = ExecutionWorker.pool # default size = number of CPU cores
      end

      def future(&block)
        @pool.future.resolve(block)
      end

      class OperationResolution < GraphQL::Query::SerialExecution::OperationResolution
        def result
          result_futures = super
          finish_all_futures(result_futures)
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
        # For each field, start async resolution.
        #
        # If there's an error during resolution, it will get raised again during `finish_all_futures`.
        # @return [Array<Celluloid::Future>] Futures for the field resolve values
        def result
          selections.map do |ast_field|
            execution_strategy.future do
              field_value = resolve_field(ast_field)
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

      class ExecutionWorker
        include Celluloid
        def resolve(proc)
          proc.call
        end
      end
    end
  end
end
