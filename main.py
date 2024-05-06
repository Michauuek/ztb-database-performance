from executor.database_context import DatabaseContext
from queries import most_popular_route_dict, avg_delay_and_rating_dict, mileage_above_avg_dict

if __name__ == '__main__':
    db_context = DatabaseContext()

    #single execution
    times = db_context.execute_all_queries(most_popular_route_dict)

    #multiple executions - 10 times
    db_context.execute_all_queries_multiple_times(most_popular_route_dict)
    db_context.execute_all_queries_multiple_times(avg_delay_and_rating_dict)
    db_context.execute_all_queries_multiple_times(mileage_above_avg_dict)
