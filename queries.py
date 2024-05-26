from database_connections import Connections, connections, mongo
from datetime import datetime

simple_query = """
    SELECT * 
    FROM ride
    WHERE ride_id < 100;"""

simple_query_postgres = """
    SELECT *
    FROM przejazd
    WHERE id_przejazdu < 100;"""

simple_query_mongo = [
    {
        "$match": {
            "ride.ride_id": {"$lt": 100}
        }
    }
]

simple_query_dict = {
    connections[Connections.POSTGRES]: simple_query_postgres,
    connections[Connections.MONGODB]: simple_query_mongo
}

avg_load_opinion_by_city = """SELECT AVG(ride.load) as srednie_obciazenie, AVG(ride.opinion) as srednia_opinia, location.city 
FROM ride
JOIN location ON ride.location_id = location.location_id
GROUP BY location.city;"""

avg_load_opinion_by_city_postgres = """SELECT AVG(przejazd.srednieoblozenie) as srednie_obciazenie, AVG(przejazd.opinia) as srednia_opinia, lokalizacja.city
FROM przejazd
JOIN lokalizacja ON przejazd.location_id = lokalizacja.location_id
GROUP BY lokalizacja.city;"""

avg_load_opinion_by_city_mongo = [
    {
        "$group": {
            "_id": "$location.city",
            "srednie_obciazenie": {"$avg": "$ride.load"},
            "srednia_opinia": {"$avg": "$ride.opinion"}
        }
    },
    {
        "$project": {
            "city": "$_id",
            "srednie_obciazenie": 1,
            "srednia_opinia": 1,
            "_id": 0
        }
    }
]

avg_load_opinion_by_city_dict = {
    connections[Connections.POSTGRES]: avg_load_opinion_by_city_postgres,
    connections[Connections.MONGODB]: avg_load_opinion_by_city_mongo
}

count_tickets_sold_one_day = """SELECT COUNT(ride.ticket_id) AS count_sold_tickets, 
       location.city, 
       vehicle.type
FROM ride
JOIN location ON ride.location_id = location.location_id
JOIN vehicle ON ride.vehicle_id = vehicle.vehicle_id
JOIN ticket ON ride.ticket_id = ticket.ticket_id
WHERE CONTAINS(ticket.date, '2023-09-08')
GROUP BY vehicle.type, location.city;"""

count_tickets_sold_one_day_postgres = """SELECT COUNT(przejazd.id_biletu) AS count_sold_tickets, lokalizacja.city, pojazd.model
FROM przejazd
JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
JOIN bilet ON przejazd.id_biletu = bilet.id_biletu
JOIN lokalizacja ON przejazd.location_id = lokalizacja.location_id
WHERE bilet.data_skasowania = '2023-09-08'
GROUP BY pojazd.model, lokalizacja.city;"""

count_tickets_sold_one_day_mongo = [
    {
        "$match": {
            "ride.ticket_details.date": {
                "$regex": ".*2023-09-08.*"
            }
        }
    },
    {
        "$group": {
            "_id": {
                "model": "$ride.vehicle.type",
                "city": "$location.city"
            },
            "count_sold_tickets": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "model": "$_id.model",
            "city": "$_id.city",
            "count_sold_tickets": 1
        }
    }
]

count_tickets_sold_one_day_dict = {
    connections[Connections.POSTGRES]: count_tickets_sold_one_day_postgres,
    connections[Connections.MONGODB]: count_tickets_sold_one_day_mongo
}

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

count_tickets_sold_one_month_postgres = """SELECT COUNT(przejazd.id_biletu) AS ilosc_biletow_sprzedanych, lokalizacja.city, pojazd.model
FROM przejazd
JOIN lokalizacja ON przejazd.location_id = lokalizacja.location_id
JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
JOIN bilet ON przejazd.id_biletu = bilet.id_biletu
WHERE EXTRACT(MONTH FROM bilet.data_skasowania) = 9
      AND EXTRACT(YEAR FROM bilet.data_skasowania) = 2023
GROUP BY pojazd.model, lokalizacja.city;
"""

count_tickets_sold_one_month_mongo = [
    {
        "$addFields": {
            "ticket_details.date_converted": {
                "$dateFromString": {
                    "dateString": "$ride.ticket_details.date",
                    "format": "%Y-%m-%d %H:%M:%S"
                }
            }
        }
    },
    {
        "$match": {
            "ticket_details.date_converted": {
                "$gte": datetime(2023, 9, 1, 0, 0, 0),
                "$lt": datetime(2023, 10, 1, 0, 0, 0)
            }
        }
    },
    {
        "$group": {
            "_id": {
                "model": "$ride.vehicle.type",
                "city": "$location.city"
            },
            "count_sold_tickets": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "model": "$_id.model",
            "city": "$_id.city",
            "count_sold_tickets": 1
        }
    }
]

count_tickets_sold_one_month_dict = {
    connections[Connections.POSTGRES]: count_tickets_sold_one_month_postgres,
    connections[Connections.MONGODB]: count_tickets_sold_one_month_mongo
}

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

count_tickets_sold_one_day_subquery_postgres = """SELECT COUNT(przejazd.id_biletu) AS ilosc_biletow_sprzedanych,
    lokalizacja.city, 
    pojazd.model
FROM przejazd
JOIN lokalizacja ON przejazd.location_id = lokalizacja.location_id
JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
JOIN bilet ON przejazd.id_biletu = bilet.id_biletu
WHERE bilet.id_biletu IN (
    SELECT sub_bilet.id_biletu
    FROM bilet AS sub_bilet
    WHERE EXTRACT(MONTH FROM sub_bilet.data_skasowania) = 9
        AND EXTRACT(YEAR FROM sub_bilet.data_skasowania) = 2023
)
GROUP BY pojazd.model, lokalizacja.city
ORDER BY lokalizacja.city ASC, pojazd.model ASC;
"""

count_tickets_sold_one_day_subquery_mongo = [
    {
        "$addFields": {
            "ticket_details.date_converted": {
                "$dateFromString": {
                    "dateString": "$ride.ticket_details.date",
                    "format": "%Y-%m-%d %H:%M:%S"
                }
            }
        }
    },
    {
        "$match": {
            "ticket_details.date_converted": {
                "$gte": datetime(2023, 9, 8, 0, 0, 0),
                "$lt": datetime(2023, 9, 9, 0, 0, 0)
            }
        }
    },
    {
        "$group": {
            "_id": {
                "model": "vehicle.model",
                "city": "location.city"
            },
            "count_sold_tickets": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "model": "$_id.model",
            "city": "$_id.city",
            "count_sold_tickets": 1
        }
    },
    {
        "$sort": {
            "city": 1,
            "model": 1
        }
    }
]

count_tickets_sold_one_day_subquery_dict = {
    connections[Connections.POSTGRES]: count_tickets_sold_one_day_subquery_postgres,
    connections[Connections.MONGODB]: count_tickets_sold_one_day_subquery_mongo
}

metro_rides_count = """SELECT COUNT(*) AS ride_count
FROM ride
WHERE vehicle_id IN (
    SELECT RAW vehicle_id
    FROM vehicle AS sub_vehicle 
    WHERE sub_vehicle.type = 'Metro'
);"""

metro_rides_count_postgres = """SELECT COUNT(*) AS ride_count
FROM przejazd
WHERE id_pojazdu IN (
    SELECT pojazd.id_pojazdu
    FROM pojazd
    WHERE pojazd.model = 'Metro'
);
"""

metro_rides_count_mongo = [
    {
        "$match": {
            "ride.vehicle.type": "Metro"
        }
    },
    {
        "$count": "ride_count"
    }
]

metro_rides_count_dict = {
    connections[Connections.POSTGRES]: metro_rides_count_postgres,
    connections[Connections.MONGODB]: metro_rides_count_mongo
}

simple_update = """UPDATE vehicle SET spots = spots + 5 WHERE vehicle_id = 132"""

simple_update_postgres = """UPDATE pojazd SET spots = spots + 5 WHERE id_pojazdu = 132;"""

simple_update_mongo = [
    {
        "ride.vehicle.vehicle_id": 132
    },
    {
        "$inc": {"ride.vehicle.spots": 5}
    }
]

simple_update_dict = {
    connections[Connections.POSTGRES]: simple_update_postgres,
    connections[Connections.MONGODB]: simple_update_mongo
}

update_tram = """UPDATE vehicle SET spots = spots+5 WHERE
vehicle_id IN (SELECT ride.vehicle_id
FROM ride
JOIN vehicle as sub_vehicle ON ride.vehicle_id = sub_vehicle.vehicle_id
WHERE sub_vehicle.type = 'Tramwaj'
GROUP BY ride.vehicle_id
HAVING AVG(ride.load) > 0.5)"""

update_tram_postgres = """UPDATE pojazd SET spots = spots + 5
WHERE id_pojazdu IN (
    SELECT przejazd.id_pojazdu
    FROM przejazd
    JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
    WHERE pojazd.model = 'Tramwaj'
    GROUP BY przejazd.id_pojazdu
    HAVING AVG(przejazd.srednieoblozenie) > 0.5
);
"""

update_tram_mongo = [
    {
        "ride.vehicle.type": "Tramwaj",
        "$expr": {"$gt": [{"$avg": "$ride.load"}, 0.5]}
    },
    {
        "$inc": {"ride.vehicle.spots": 5}
    }
]

update_tram_dict = {
    connections[Connections.POSTGRES]: update_tram_postgres,
    connections[Connections.MONGODB]: update_tram_mongo
}

insert_ride = """INSERT INTO `ride` (KEY, VALUE)
VALUES ('500000', { "late" : 1, "late_time": 3, "load": 0.48, "location_id": 33, "opinion": 4.64, "ride_id": 4753133, "route_id": 44753512, "ticket_id": 753874, "vehicle_id": 184 })"""

insert_ride_postgres = """INSERT INTO przejazd (id_przejazdu, opoznienie, srednieoblozenie, opinia, id_trasa, id_biletu, id_pojazdu, location_id)
VALUES ('600005', 1, 0.41, 44, 753874, 184, 123, 8);
"""

insert_ride_mongo = [
    {
        "route_id": 441,
        "start_station": "Skwer Słoneczny",
        "end_station": "Rondo Targowe",
        "distance": 3.3,
        "location": {
            "location_id": 4,
            "city": "Toruń",
            "voivodeship": "kujawsko-pomorskie"
        },
        "ride": {
            "ride_id": 600004,
            "vehicle": {
                "vehicle_id": 74,
                "type": "Autobus",
                "line_num": 11,
                "vacation_rides": 0,
                "fuel_usage": 43,
                "fuel": "Benzyna",
                "spots": 50
            },
            "load": 0.15,
            "delay": 56,
            "opinion": 2.67,
            "ticket_details": {
                "ticket_id": 1,
                "date": "2021-07-05 16:34:37",
                "ticket_type_id": 7,
                "type": "Grupowy",
                "cost": 20,
                "valid_for": 60,
                "zone": "I"
            }
        }
    }
]

insert_ride_dict = {
    connections[Connections.POSTGRES]: insert_ride_postgres,
    connections[Connections.MONGODB]: insert_ride_mongo
}

delete_simple = """DELETE
FROM `ride` AS ride
WHERE ride.vehicle_id = 132 AND late = 1"""

delete_simple_postgres = """DELETE
FROM przejazd
WHERE id_pojazdu = 132 AND opoznienie = 0;
"""

delete_simple_mongo = {
    "ride.vehicle_id": 132,
    "ride.delay": 0
}

delete_simple_dict = {
    connections[Connections.POSTGRES]: delete_simple_postgres,
    connections[Connections.MONGODB]: delete_simple_mongo
}

delete_complex = """DELETE
FROM `ride` AS ride
WHERE ride.vehicle_id IN (
    SELECT RAW vehicle_id
    FROM vehicle AS sub_vehicle
    WHERE sub_vehicle.type = 'Tramwaj'
)
AND ride_id IN (
    SELECT RAW ride_id
    FROM ride AS sub_ride
    WHERE sub_ride.opinion > 3.5
)
AND late = 1
AND ticket_id IN (4, 37, 94);
"""

delete_complex_postgres = """DELETE
FROM przejazd
WHERE id_pojazdu IN (
    SELECT pojazd.id_pojazdu
    FROM pojazd
    WHERE pojazd.model = 'Tramwaj'
)
AND id_przejazdu IN (
    SELECT przejazd.id_przejazdu
    FROM przejazd
    WHERE przejazd.opinia > 3.5
)
AND opoznienie = 0 
AND id_biletu IN (4, 37, 94);
"""

delete_complex_mongo = {
    "ride.vehicle.type": "Tramwaj",
    "ride.opinion": {"$gt": 3.5},
    "ride.delay": 0,
    "ride.ticket_id": {"$in": [4, 37, 94]}
}

delete_complex_dict = {
    connections[Connections.POSTGRES]: delete_complex_postgres,
    connections[Connections.MONGODB]: delete_complex_mongo
}
