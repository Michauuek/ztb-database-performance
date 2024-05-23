from executor.mongo_query_executor import MongoQueryExecutor
from executor.postgres_query_executor import PostgresQueryExecutor

from enum import Enum


class Connections(Enum):
    POSTGRES = 1
    COUCHDB = 2
    SQLITE = 3
    MONGODB = 4


postgres = PostgresQueryExecutor(
    "transport",
    "postgres",
    "tajnedane",
    "localhost",
    "5432")

mongo = MongoQueryExecutor(
    "mongodb://user:password@localhost:27017",
    "transport3",
    "transport")


connections = {
    Connections.POSTGRES: postgres,
    Connections.MONGODB: mongo,
    #TODO: Add other connections
}
