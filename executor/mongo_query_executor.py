import pymongo
from executor.database_query_executor import DatabaseQueryExecutor


class MongoQueryExecutor(DatabaseQueryExecutor):
    def __init__(self, uri, dbname, collection_name):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[dbname]
        self.collection = self.db[collection_name]

    def execute_query(self, query):
        result = list(self.collection.aggregate(query))
        print(len(result))
        return result

    def execute_without_fetch(self, insert_document, action=0):
        if action == 0:
            self.collection.insert_many(insert_document)
        elif action == 1:
            self.collection.delete_many(insert_document)
        elif action == 2:
            self.collection.update_many(insert_document[0], insert_document[1])

    def close(self):
        self.client.close()
