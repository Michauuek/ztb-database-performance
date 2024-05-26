import pandas as pd
import numpy as np
import json



# Load Excel files into pandas DataFrames
vehicles_df = pd.read_csv('transportation_vehicle.csv')
routes_df = pd.read_csv('route.csv')
journeys_df = pd.read_csv('ride.csv')
tickets_df = pd.read_csv('ticket_sa.csv')
ticket_types_df = pd.read_csv('ticket_type.csv')
locations_df = pd.read_csv('location.csv')


# Generate a random 'ticket_type_id' for each ticket in 'tickets_df'
np.random.seed(42) 
tickets_df['ticket_type_id'] = np.random.randint(1, 8, size=len(tickets_df))
journeys_df['location_id'] = np.random.randint(1, 32, size=len(journeys_df))

# Merge tickets_df with ticket_types_df to include ticket type details in tickets_df
tickets_df = pd.merge(tickets_df, ticket_types_df, on='ticket_type_id', how='left')

excel_data = []

for _, route_row in routes_df.iterrows():
    route_journeys = journeys_df[journeys_df['route_id'] == route_row['route_id']]
    
    for _, journey_row in route_journeys.iterrows():
        journey_tickets = tickets_df[tickets_df['ticket_id'] == journey_row['ticket_id']]
        ticket_details = {}
        
        for _, ticket_row in journey_tickets.iterrows():
            ticket_dict = {
                "ticket_id": ticket_row['ticket_id'],
                "date": ticket_row['date'],
                "ticket_type_id": ticket_row['ticket_type_id'],
                "type": ticket_row['type'],
                "cost": ticket_row['cost'],
                "valid_for": ticket_row['valid_for'],
                "zone": ticket_row['zone'],
            }
            ticket_details = ticket_dict


        location_json = {}
        for _, location_row in locations_df.iterrows():
            if location_row['location_id'] == journey_row['location_id']:
                location_dict = {
                    "location_id": location_row['location_id'],
                    "city": location_row['city'],
                    "voivodeship": location_row['voivodeship'],
                }
                location_json = json.dumps(location_dict)



        vehicle_dict = {}
        for _, vehicle_row in vehicles_df.iterrows():
            if vehicle_row['vehicle_id'] == journey_row['vehicle_id']:
                vehicle_dict = {
                    "vehicle_id": vehicle_row['vehicle_id'],
                    "type": vehicle_row['type'],
                    "line_num": vehicle_row['line_num'],
                    "vacation_rides": vehicle_row['vacation_rides'],
                    "fuel_usage": vehicle_row['fuel_usage'],
                    "fuel": vehicle_row['fuel'],
                    "spots": vehicle_row['spots'],
                }
                vehicle_json = json.dumps(vehicle_dict)


        print(location_json)
        print(vehicle_json)

        journey_dict = {
            "ride_id": journey_row['ride_id'],
            "vehicle": vehicle_dict,
            "load": journey_row['load'],
            "delay": 0 if pd.isna(journey_row['late_time']) else journey_row['late_time'],
            "opinion": journey_row['opinion'],
            "ticket_details": ticket_details,
        }

        excel_data.append({
            "route_id": route_row['route_id'],
            "start_station": route_row['start_station'],
            "end_station": route_row['finish_station'],
            "distance": route_row['length'],
            "location": location_json,
            "ride": json.dumps(journey_dict)
        })

# Convert the list of dictionaries to a DataFrame
final_df = pd.DataFrame(excel_data)
final_df.head()

# Save the DataFrame to an Excel file
final_df.to_csv('transport_data_final.csv', index=False)

print("Excel file has been successfully created.")