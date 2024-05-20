from executor.database_context import DatabaseContext
from queries import most_popular_route_dict, mileage_above_avg_dict, \
    avg_load_opinion_by_city_dict, count_tickets_sold_one_day_dict, count_tickets_sold_one_month_dict, \
    count_tickets_sold_one_day_subquery_dict, metro_rides_count_dict, insert_ride_dict

if __name__ == '__main__':
    db_context = DatabaseContext()

    times = db_context.execute_all_queries(insert_ride_dict)
    print(times)
