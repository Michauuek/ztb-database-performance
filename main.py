from executor.database_context import DatabaseContext
from queries import most_popular_route_dict

if __name__ == '__main__':
    db_context = DatabaseContext()
    times = db_context.execute_all_queries(most_popular_route_dict)
    print(times)
