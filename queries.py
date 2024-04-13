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

#TODO: Implement real query for MongoDB
most_popular_route_query_mongo = {"_id": "660adeedfca47a474361b841"}


most_popular_route_dict = {
    connections[Connections.POSTGRES]: most_popular_route_query_postgres,
    connections[Connections.MONGODB]: most_popular_route_query_mongo
}