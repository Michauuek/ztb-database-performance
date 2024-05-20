from database_connections import Connections, connections
from datetime import datetime

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
    {
        "$group": {
            "_id": "$id_trasa",
            "ticket_sales": {"$sum": 1}
        }
    },
    {
        "$sort": {"ticket_sales": -1}
    },
    {
        "$limit": 1
    },
    {
        "$project": {
            "_id": 0,
            "id_trasa": "$_id",
            "ticket_sales": 1
        }
    }
]

most_popular_route_dict = {
    connections[Connections.POSTGRES]: most_popular_route_query_postgres,
    connections[Connections.MONGODB]: most_popular_route_query_mongo
}

mileage_above_avg_postgres = """
    SELECT poj.model, poj.spalanie, AVG(pr.opoznienie) AS average_delay
    FROM pojazd poj
    JOIN przejazd pr ON poj.id_pojazdu = pr.id_pojazdu
    WHERE poj.spalanie > (SELECT AVG(spalanie) FROM pojazd)
    GROUP BY poj.model, poj.spalanie;
"""

mileage_above_avg_dict = {
    connections[Connections.POSTGRES]: mileage_above_avg_postgres
}

avg_load_opinion_by_city = """SELECT AVG(ride.load) as srednie_obciazenie, AVG(ride.opinion) as srednia_opinia, location.city 
FROM ride
JOIN location ON ride.location_id = location.location_id
GROUP BY location.city;"""

avg_load_opinion_by_city_postgres = """SELECT AVG(przejazd.srednieoblozenie) as srednie_obciazenie, AVG(przejazd.opinia) as srednia_opinia, trasa.przystanekpoczatkowy, trasa.przystanekkoncowy
FROM przejazd
JOIN trasa ON przejazd.id_trasa = trasa.id_trasa
GROUP BY trasa.przystanekpoczatkowy, trasa.przystanekkoncowy;"""

avg_load_opinion_by_city_dict = {
    connections[Connections.POSTGRES]: avg_load_opinion_by_city_postgres
}

count_tickets_sold_one_day = """SELECT COUNT(ride.ticket_id) AS count_sold_tickets, vehicle.type
FROM ride
JOIN vehicle ON ride.vehicle_id = vehicle.vehicle_id
JOIN ticket ON ride.ticket_id = ticket.ticket_id
WHERE CONTAINS(ticket.date, '2023-09-08')
GROUP BY vehicle.type;"""

count_tickets_sold_one_day_postgres = """SELECT COUNT(przejazd.id_biletu) AS count_sold_tickets, pojazd.model
FROM przejazd
JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
JOIN bilet ON przejazd.id_biletu = bilet.id_biletu
WHERE bilet.data_skasowania = '2023-09-08'
GROUP BY pojazd.model;"""

count_tickets_sold_one_day_mongo = [
    {
        "$match": {
            "data_skasowania": "2023-09-08"
        }
    },
    {
        "$group": {
            "_id": "$model",
            "count_sold_tickets": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    },
    {
        "$project": {
            "_id": 0,
            "model": "$_id",
            "count_sold_tickets": 1
        }
    }
];

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

count_tickets_sold_one_month_postgres = """SELECT COUNT(przejazd.id_biletu) AS ilosc_biletow_sprzedanych, pojazd.model
FROM przejazd
JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
JOIN bilet ON przejazd.id_biletu = bilet.id_biletu
WHERE EXTRACT(MONTH FROM bilet.data_skasowania) = 9
      AND EXTRACT(YEAR FROM bilet.data_skasowania) = 2023
    GROUP BY pojazd.model;
"""

count_tickets_sold_one_month_mongo = [
    {
        "$match": {
            "data_skasowania": {
                "$gte": datetime(2023, 9, 1, 0, 0, 0),
                "$lt": datetime(2023, 10, 1, 0, 0, 0)
            }
        }
    },
    {
        "$group": {
            "_id": "$model",
            "ilosc_biletow_sprzedanych": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    },
    {
        "$project": {
            "_id": 0,
            "model": "$_id",
            "ilosc_biletow_sprzedanych": 1
        }
    }
];

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

count_tickets_sold_one_day_subquery_postgres = """SELECT COUNT(przejazd.id_biletu) AS ilosc_biletow_sprzedanych, pojazd.model
FROM przejazd
JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
JOIN bilet ON przejazd.id_biletu = bilet.id_biletu
WHERE bilet.id_biletu IN (
    SELECT sub_bilet.id_biletu
    FROM bilet AS sub_bilet
    WHERE EXTRACT(MONTH FROM sub_bilet.data_skasowania) = 9
        AND EXTRACT(YEAR FROM sub_bilet.data_skasowania) = 2023
)
GROUP BY pojazd.model
ORDER BY pojazd.model ASC;
"""

count_tickets_sold_one_day_subquery_mongo = [
    {
        "$match": {
            "data_skasowania": {
                "$gte": datetime(2023, 9, 8, 0, 0, 0),
                "$lt": datetime(2023, 9, 9, 0, 0, 0)
            }
        }
    },
    {
        "$group": {
            "_id": "$model",
            "ilosc_biletow_sprzedanych": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    },
    {
        "$project": {
            "_id": 0,
            "model": "$_id",
            "ilosc_biletow_sprzedanych": 1
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

metro_rides_count_postgres = """SELECT COUNT(przejazd.id_przejazdu) AS ride_count
FROM przejazd
WHERE id_pojazdu IN (
    SELECT pojazd.id_pojazdu
    FROM pojazd
    WHERE pojazd.model = 'Metro'
);
"""

metro_rides_count_mongo = [
    {
        "$lookup": {
            "from": "pojazd",
            "localField": "id_pojazdu",
            "foreignField": "id_pojazdu",
            "as": "pojazd_info"
        }
    },
    {
        "$match": {
            "pojazd_info.model": "Metro"
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

update_tram = """UPDATE vehicle SET spots = spots+5 WHERE
vehicle_id IN (SELECT ride.vehicle_id
FROM ride
JOIN vehicle as sub_vehicle ON ride.vehicle_id = sub_vehicle.vehicle_id
WHERE sub_vehicle.type = 'Tramwaj'
GROUP BY ride.vehicle_id
HAVING AVG(ride.load) > 0.5)"""

update_tram_postgres = """UPDATE pojazd SET ilosc_miejsc = ilosc_miejsc + 5
WHERE id_pojazdu IN (
    SELECT przejazd.id_pojazdu
    FROM przejazd
    JOIN pojazd ON przejazd.id_pojazdu = pojazd.id_pojazdu
    WHERE pojazd.model = 'Tramwaj'
    GROUP BY przejazd.id_pojazdu
    HAVING AVG(przejazd.obciazenie) > 0.5
);
"""

update_tram_dict = {
    connections[Connections.POSTGRES]: update_tram_postgres
}

insert_ride = """INSERT INTO `ride` (KEY, VALUE)
VALUES ('500000', { "late" : 1, "late_time": 3, "load": 0.48, "location_id": 33, "opinion": 4.64, "ride_id": 4753133, "route_id": 44753512, "ticket_id": 753874, "vehicle_id": 184 })"""

insert_ride_postgres = """INSERT INTO przejazd (id_przejazdu, opoznienie, srednieoblozenie, opinia, id_trasa, id_biletu, id_pojazdu)
VALUES (5000000, 1, 0.41, 44, 753874, 184, 123);
"""

insert_ride_mongo = [
    {
        "$insertOne": {
            "document": {
                "id_przejazdu": 5000000,
                "opoznienie": 1,
                "srednieoblozenie": 0.41,
                "opinia": 44,
                "id_trasa": 753874,
                "id_biletu": 184,
                "id_pojazdu": 123
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
WHERE ride.vehicle_id = 132 AND late > 10"""

delete_complex = """DELETE
FROM `ride` AS ride
WHERE ride.vehicle_id IN (
    SELECT RAW vehicle_id
    FROM vehicle AS sub_vehicle
    WHERE sub_vehicle.type = 'Tramwaj'
)
AND late = 1
AND ride_id IN (
    SELECT RAW ride_id
    FROM ride AS sub_ride
    WHERE sub_ride.opinion > 3.5
)
"""

delete_simple_postgres = """DELETE
FROM przejazd
WHERE id_pojazdu = 132 AND opoznienie > 10;
"""

delete_simple_mongo = [
    {
        "$match": {
            "id_pojazdu": 132,
            "opoznienie": { "$gt": 10 }
        }
    },
    {
        "$delete": {}
    }
]

delete_complex_postgres = """DELETE
FROM przejazd
WHERE id_pojazdu IN (
    SELECT pojazd.id_pojazdu
    FROM pojazd
    WHERE pojazd.model = 'Tramwaj'
)
AND opoznienie = 1
AND id_przejazdu IN (
    SELECT przejazd.id_przejazdu
    FROM przejazd
    WHERE przejazd.opinia > 3.5
);
"""

delete_simple_dict = {
    connections[Connections.POSTGRES]: delete_simple_postgres,
    connections[Connections.MONGODB]: delete_simple_mongo
}

delete_complex_dict = {
    connections[Connections.POSTGRES]: delete_complex_postgres
}
