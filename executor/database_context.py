from executor.database_query_executor import DatabaseQueryExecutor


class DatabaseContext:
    """ Manages the query strategy to use for executing database queries. """

    def __init__(self):
        self.strategy = None

    def set_strategy(self, strategy):
        self.strategy = strategy

    def execute_query(self, query):
        return self.strategy.measure_query_time(query)

    def get_query_result(self, query):
        return self.strategy.execute_query(query)

    def execute_all_queries(self, query_dict: dict[DatabaseQueryExecutor, str]):
        results = {}
        for strategy, query in query_dict.items():
            self.strategy = strategy
            results[type(strategy).__name__] = self.execute_query(query)
        return results

    def close(self):
        self.strategy.close()
