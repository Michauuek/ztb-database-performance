from executor.database_context import DatabaseContext
from queries import most_popular_route_dict, mileage_above_avg_dict, \
    avg_load_opinion_by_city_dict, count_tickets_sold_one_day_dict, count_tickets_sold_one_month_dict, \
    count_tickets_sold_one_day_subquery_dict, metro_rides_count_dict, insert_ride_dict, delete_simple_dict, \
    update_tram_dict

if __name__ == '__main__':
    db_context = DatabaseContext()

    times = db_context.execute_all_queries(metro_rides_count_dict)
    print(times)

    times_insert = db_context.execute_all_queries_without_fetch(insert_ride_dict, action=0)
    print(times_insert)

    times_delete = db_context.execute_all_queries_without_fetch(delete_simple_dict, action=1)
    print(times_delete)

    times_update = db_context.execute_all_queries_without_fetch(update_tram_dict, action=2)
    print(times_update)

