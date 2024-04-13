from database_connections import Connections, connections

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


most_popular_route_dict = {
    connections[Connections.POSTGRES]: most_popular_route_query_postgres,
    connections[Connections.MONGODB]: most_popular_route_query_mongo
}