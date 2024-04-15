from datetime import timedelta
import time
from executor.database_query_executor import DatabaseQueryExecutor
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions

class CouchbaseQueryExecutor(DatabaseQueryExecutor):
    def __init__(self, username, password, bucket_name):
        self.authenticator = PasswordAuthenticator(username, password)
        self.cluster = Cluster('couchbase://localhost', ClusterOptions(self.authenticator))
        self.cluster.wait_until_ready(timedelta(seconds=5))
        self.bucket = self.cluster.bucket(bucket_name)
        self.collection = self.bucket.default_collection()

    def execute_query(self, query):
        result = self.cluster.query(query)
        return result

    def close(self):
        self.cluster.disconnect()