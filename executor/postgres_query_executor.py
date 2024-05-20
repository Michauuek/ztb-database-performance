from executor.database_query_executor import DatabaseQueryExecutor
import psycopg2


class PostgresQueryExecutor(DatabaseQueryExecutor):
    def __init__(self, dbname, user, password, host, port):
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            print(e)
            return None

    def close(self):
        self.cursor.close()
        self.connection.close()
