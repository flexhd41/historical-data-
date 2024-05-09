import requests
import psycopg2
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os
import json
from decimal import Decimal

# Load environment variables from .env file
load_dotenv()

# Function to fetch planet data from the API
def fetch_planet_data():
    url = "https://api.enshrouded.eu/api/v1/campaigns"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from the API.")
        return []

def fetch_weighted_average_health_last_60_minutes(planet_name):
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT")
    )
    c = conn.cursor()

    # Calculate the timestamp for 60 minutes ago
    sixty_minutes_ago = datetime.now() - timedelta(minutes=60)

    # Query to fetch health values over the last 60 minutes
    c.execute("SELECT health, timestamp FROM planet_data WHERE name = %s AND timestamp >= %s ORDER BY timestamp ASC",
              (planet_name, sixty_minutes_ago))
    results = c.fetchall()

    conn.close()

    if results:
        # Calculate the weighted average health over the last 60 minutes
        total_health = Decimal(0)
        total_weight = Decimal(0)
        for result in results:
            health, timestamp = result
            # Assuming a simple linear weighting based on time difference
            weight = (datetime.now() - timestamp).total_seconds() / 3600 # Weight based on time difference in hours
            # Convert weight to Decimal to match the type of health
            weight = Decimal(weight)
            total_health += Decimal(health) * weight
            total_weight += weight

        weighted_average_health = total_health / total_weight if total_weight > 0 else 0
        return weighted_average_health
    else:
        # If no records are found, return None
        return None


def store_planet_data(campaign_data):
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT")
    )
    c = conn.cursor()

    # Adjusted CREATE TABLE statement to ensure health fields are stored as numeric
    c.execute('''CREATE TABLE IF NOT EXISTS planet_data
                 (name TEXT, currentOwner TEXT, regenPerSecond REAL, playerCount INTEGER, maxHealth NUMERIC, health NUMERIC, statistics JSONB, timestamp TIMESTAMP, liberation REAL, percentage_gain_per_hour REAL)''')

    # Create campaign_data table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS campaign_data
                 (id INTEGER, eventType INTEGER, faction TEXT, health NUMERIC, maxHealth NUMERIC, startTime TIMESTAMP, endTime TIMESTAMP, campaignId INTEGER, jointOperationIds TEXT[], statistics JSONB, type INTEGER, count INTEGER, liberation INTEGER)''')

    for campaign in campaign_data:
        planet = campaign['planet']
        # Calculate liberation percentage
        liberation = (planet['health'] / planet['maxHealth']) * 100 if planet['maxHealth'] > 0 else 0

        # Fetch the weighted average health over the last 60 minutes
        weighted_average_health_last_60_minutes = fetch_weighted_average_health_last_60_minutes(planet['name'])
        if weighted_average_health_last_60_minutes is not None:
            # Calculate expected health gain over the hour based on regenPerSecond
            expected_health_gain_per_hour = Decimal(planet['regenPerSecond']) * Decimal(3600)

            # Calculate actual health gain
            actual_health_gain = Decimal(planet['health']) - weighted_average_health_last_60_minutes

            # Calculate percentage gain per hour and round it
            percentage_gain_per_hour = (actual_health_gain / expected_health_gain_per_hour) * 100 if expected_health_gain_per_hour > 0 else 0
            percentage_gain_per_hour = round(percentage_gain_per_hour, 2) # Rounding to 2 decimal places

            print(f"Percentage gain per hour for {planet['name']}: {percentage_gain_per_hour}%")

            # Insert planet data with all the statistics, planet health, current health, liberation percentage, and regenPerSecond
            c.execute("INSERT INTO planet_data (name, currentOwner, regenPerSecond, playerCount, maxHealth, health, statistics, timestamp, liberation, percentage_gain_per_hour) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                     (planet['name'], planet['currentOwner'], planet['regenPerSecond'], planet['statistics']['playerCount'], planet['maxHealth'], planet['health'], json.dumps(planet['statistics']), datetime.now(), liberation, percentage_gain_per_hour))

        # Check if 'event' key exists in the planet data and insert it into campaign_data if it does
        if 'event' in planet and planet['event'] is not None:
            event = planet['event']
            c.execute("INSERT INTO campaign_data (id, eventType, faction, health, maxHealth, startTime, endTime, campaignId, jointOperationIds, statistics, type, count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                     (event['id'], event['eventType'], event['faction'], event['health'], event['maxHealth'], event['startTime'], event['endTime'], event['campaignId'], event['jointOperationIds'], json.dumps(event), campaign['type'], campaign['count']))

    conn.commit()
    conn.close()




# Function to delete data older than a month
def delete_old_data():
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT")
    )
    c = conn.cursor()
    # Calculate the date 30 days ago
    one_year_ago = datetime.now() - timedelta(days=365)
    # Delete rows older than 365 days
    c.execute("DELETE FROM planet_data WHERE timestamp < %s", (one_year_ago,))
    conn.commit()
    conn.close()

# Main function to run the script
def main():
    while True:
        campaign_data = fetch_planet_data()
        print (campaign_data)
        if campaign_data:
            store_planet_data(campaign_data)
            print("Planet data stored successfully.")
            delete_old_data()
            print("Old data deleted successfully.")
        else:
            print("No data to store.")
        time.sleep(60) # Wait for 60 seconds before the next iteration

if __name__ == "__main__":
    main()
