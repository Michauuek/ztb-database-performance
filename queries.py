#from database_connections import Connections, connections

most_popular_route_query_postgres = """
    SELECT tr.id_trasa, COUNT(b.id_biletu) AS ticket_sales
    FROM trasa tr
    JOIN przejazd p ON tr.id_trasa = p.id_trasa
    JOIN bilet b ON p.id_biletu = b.id_biletu
    GROUP BY tr.id_trasa
    ORDER BY ticket_sales DESC
    LIMIT 1;
"""

most_popular_route_query_mongo = [
    {"$group": {"_id": "$route_id", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
]


# most_popular_route_dict = {
#     connections[Connections.POSTGRES]: most_popular_route_query_postgres,
#     connections[Connections.MONGODB]: most_popular_route_query_mongo
# }


avg_delay_and_rating_postgres = """
    SELECT poj.model, AVG(pr.opoznienie) AS average_delay, AVG(pr.opinia) AS average_opinion
    FROM pojazd poj
    JOIN przejazd pr ON poj.id_pojazdu = pr.id_pojazdu
    GROUP BY poj.model;
"""

# avg_delay_and_rating_dict = {
#     connections[Connections.POSTGRES]: avg_delay_and_rating_postgres
# }

mileage_above_avg_postgres = """
    SELECT poj.model, poj.spalanie, AVG(pr.opoznienie) AS average_delay
    FROM pojazd poj
    JOIN przejazd pr ON poj.id_pojazdu = pr.id_pojazdu
    WHERE poj.spalanie > (SELECT AVG(spalanie) FROM pojazd)
    GROUP BY poj.model, poj.spalanie;
"""

# mileage_above_avg_dict = {
#     connections[Connections.POSTGRES]: mileage_above_avg_postgres
# }

avg_load_opinion_by_city = """SELECT AVG(ride.load) as srednie_obciazenie, AVG(ride.opinion) as srednia_opinia, location.city 
FROM ride
JOIN location ON ride.location_id = location.location_id
GROUP BY location.city;"""

count_tickets_sold_one_day = """SELECT COUNT(ride.ticket_id) AS count_sold_tickets, 
       location.city, 
       vehicle.type
FROM ride
JOIN location ON ride.location_id = location.location_id
JOIN vehicle ON ride.vehicle_id = vehicle.vehicle_id
JOIN ticket ON ride.ticket_id = ticket.ticket_id
WHERE CONTAINS(ticket.date, '2023-09-08')
GROUP BY vehicle.type, location.city;"""

count_tickets_sold_one_month = """SELECT COUNT(ride.ticket_id) AS ilosc_biletow_sprzedanych, 
       location.city, 
       vehicle.type
FROM ride
JOIN location ON ride.location_id = location.location_id
JOIN vehicle ON ride.vehicle_id = vehicle.vehicle_id
JOIN ticket AS main_ticket ON ride.ticket_id = main_ticket.ticket_id
WHERE DATE_PART_STR(main_ticket.date, 'month') = 9
  AND DATE_PART_STR(main_ticket.date, 'year') = 2023
GROUP BY vehicle.type, location.city;"""

count_tickets_sold_one_day_subquery = """SELECT COUNT(ride.ticket_id) AS ilosc_biletow_sprzedanych, 
       location.city, 
       vehicle.type
FROM ride
JOIN location ON ride.location_id = location.location_id
JOIN vehicle ON ride.vehicle_id = vehicle.vehicle_id
JOIN ticket ON ride.ticket_id = ticket.ticket_id
WHERE ticket.ticket_id IN (
    SELECT RAW sub_ticket.ticket_id 
    FROM ticket AS sub_ticket 
    WHERE DATE_PART_STR(sub_ticket.date, 'month') = 9
      AND DATE_PART_STR(sub_ticket.date, 'year') = 2023
)
GROUP BY vehicle.type, location.city
ORDER BY location.city ASC, vehicle.type DESC;"""

metro_rides_count = """SELECT COUNT(*) AS ride_count
FROM ride
WHERE vehicle_id IN (
    SELECT RAW vehicle_id
    FROM vehicle AS sub_vehicle 
    WHERE sub_vehicle.type = 'Metro'
);"""

update_tram = """UPDATE vehicle SET spots = spots+5 WHERE
vehicle_id IN (SELECT ride.vehicle_id
FROM ride
JOIN vehicle as sub_vehicle ON ride.vehicle_id = sub_vehicle.vehicle_id
WHERE sub_vehicle.type = 'Tramwaj'
GROUP BY ride.vehicle_id
HAVING AVG(ride.load) > 0.5)"""

insert_location = """INSERT INTO `location` (KEY, VALUE)
VALUES ('33', { "location_id": 34, "city": "Zgierz", "voivodeship": "lodzkie" });"""

insert_ride = """INSERT INTO `ride` (KEY, VALUE)
VALUES ('500000', { "late" : 1, "late_time": 3, "load": 0.48, "location_id": 33, "opinion": 4.64, "ride_id": 4753133, "route_id": 44753512, "ticket_id": 753874, "vehicle_id": 184 })"""

delete_simple = """DELETE
FROM `ride` AS ride
WHERE ride.vehicle_id = 132 AND late > 10"""