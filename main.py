from executor.database_context import DatabaseContext
from queries import avg_load_opinion_by_city_dict, count_tickets_sold_one_day_dict, count_tickets_sold_one_month_dict, \
    count_tickets_sold_one_day_subquery_dict, metro_rides_count_dict, insert_ride_dict, delete_simple_dict, \
    update_tram_dict, simple_query_dict, simple_update_dict, delete_complex_dict

if __name__ == '__main__':
    db_context = DatabaseContext()

    simple_query_times = db_context.execute_all_queries(simple_query_dict)
    print(simple_query_times)

    avg_load_opinion_by_city_times = db_context.execute_all_queries(avg_load_opinion_by_city_dict)
    print(avg_load_opinion_by_city_times)

    count_tickets_sold_one_day_times = db_context.execute_all_queries(count_tickets_sold_one_day_dict)
    print(count_tickets_sold_one_day_times)

    count_tickets_sold_one_month_times = db_context.execute_all_queries(count_tickets_sold_one_month_dict)
    print(count_tickets_sold_one_month_times)

    count_tickets_sold_one_day_subquery_times = db_context.execute_all_queries(count_tickets_sold_one_day_subquery_dict)
    print(count_tickets_sold_one_day_subquery_times)

    metro_rides_count_times = db_context.execute_all_queries(metro_rides_count_dict)
    print(metro_rides_count_times)

    times_insert = db_context.execute_all_queries_without_fetch(insert_ride_dict, action=0)
    print(times_insert)

    times_update = db_context.execute_all_queries_without_fetch(update_tram_dict, action=2)
    print(times_update)

    times_simple_update = db_context.execute_all_queries_without_fetch(simple_update_dict, action=2)
    print(times_simple_update)

    times_delete = db_context.execute_all_queries_without_fetch(delete_simple_dict, action=1)
    print(times_delete)

    delete_complex_times = db_context.execute_all_queries_without_fetch(delete_complex_dict, action=1)
    print(delete_complex_times)


