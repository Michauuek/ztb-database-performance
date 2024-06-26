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
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(len(result))
        return result

    def execute_without_fetch(self, insert_statement, action=0):
        self.cursor.execute(insert_statement)
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()
