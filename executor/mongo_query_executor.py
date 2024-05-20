import pymongo
from executor.database_query_executor import DatabaseQueryExecutor


class MongoQueryExecutor(DatabaseQueryExecutor):
    def __init__(self, uri, dbname, collection_name):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[dbname]
        self.collection = self.db[collection_name]

    def execute_query(self, query):
        return list(self.collection.aggregate(query))

    def execute_insert_query(self, query):
        return self.collection.insert_many(query)

    def close(self):
        self.client.close()