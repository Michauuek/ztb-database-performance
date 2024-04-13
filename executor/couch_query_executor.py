from executor.database_query_executor import DatabaseQueryExecutor
import couchdb


class CouchDBQueryExecutor(DatabaseQueryExecutor):
    def __init__(self, url, dbname):
        self.server = couchdb.Server(url)
        self.db = self.server[dbname]

    #TODO: Implement the execute_query method
    def execute_query(self, query):
        pass

    def close(self):
        pass