import concurrent.futures
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

    def execute_all_queries_without_fetch(self, query_dict: dict[DatabaseQueryExecutor, str], action: int = 0):
        results = {}
        for strategy, query in query_dict.items():
            self.strategy = strategy
            results[type(strategy).__name__] = self.strategy.measure_insert_time(query, action)
        return results

    def execute_all_queries_multiple_times(self, query_dict: dict[DatabaseQueryExecutor, str]):
        results = {}
        for strategy, query in query_dict.items():
            self.strategy = strategy
            results[type(strategy).__name__] = self.execute_query_multiple_times(query)
        print(results)
        return results

    def execute_query_multiple_times_concurrently(self, query, times=10, use_threads=True):
        with concurrent.futures.ThreadPoolExecutor() if use_threads else concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [executor.submit(self.execute_query, query) for _ in range(times)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
            print(results)
        average_time = sum(results) / len(results)
        return average_time

    def execute_query_multiple_times(self, query, times=10):
        total_time = 0
        for _ in range(times):
            execution_time = self.execute_query(query)
            total_time += execution_time
        average_time = total_time / times
        return average_time

    def close(self):
        self.strategy.close()
