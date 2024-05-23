import time
from abc import ABC, abstractmethod


class DatabaseQueryExecutor(ABC):

    @abstractmethod
    def execute_query(self, query):
        pass

    @abstractmethod
    def execute_without_fetch(self, insert_statement, action=0):
        pass

    @abstractmethod
    def close(self):
        pass

    def measure_query_time(self, query):
        start_time = time.time()
        self.execute_query(query)
        elapsed_time = time.time() - start_time
        return "{:.6f}".format(elapsed_time)

    def measure_insert_time(self, insert_statement, action=0):
        start_time = time.time()
        self.execute_without_fetch(insert_statement, action)
        elapsed_time = time.time() - start_time
        return "{:.6f}".format(elapsed_time)



