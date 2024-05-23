from executor.database_query_executor import DatabaseQueryExecutor
import sqlite3


class SQLiteQueryExecutor(DatabaseQueryExecutor):
    def __init__(self, db_file):
        self.connection = sqlite3.connect(db_file)
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def execute_without_fetch(self, insert_statement, action=0):
        self.cursor.execute(insert_statement)
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()
