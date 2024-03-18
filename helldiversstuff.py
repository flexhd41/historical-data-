import requests
import psycopg2
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Function to fetch planet data from the API
def fetch_planet_data():
    url = "http://dev.nexusrealms.de:25567/api/planet-data"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from the API.")
        return []

# Function to store planet data in the PostgreSQL database
def store_planet_data(planet_data):
    # Use environment variables for database connection
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT")
    )
    c = conn.cursor()

    # Create table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS planet_data
                 (name TEXT, liberation REAL, players INTEGER, regen_per_hour_percent REAL, regen_per_hour_hp REAL, timestamp TIMESTAMP)''')

    # Insert planet data
    for planet in planet_data:
        c.execute("INSERT INTO planet_data (name, liberation, players, regen_per_hour_percent, regen_per_hour_hp, timestamp) VALUES (%s, %s, %s, %s, %s, %s)",
                 (planet['name'], float(planet['liberation'].strip('%')), planet['players'], float(planet['regen_per_hour_percent'].replace('%/hr', '')), float(planet['regen_per_hour_hp'].strip('HP/hr')), datetime.now()))

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
    thirty_days_ago = datetime.now() - timedelta(days=30)
    # Delete rows older than 30 days
    c.execute("DELETE FROM planet_data WHERE timestamp < %s", (thirty_days_ago,))
    conn.commit()
    conn.close()

# Main function to run the script
def main():
    while True:
        planet_data = fetch_planet_data()
        if planet_data:
            store_planet_data(planet_data)
            print("Planet data stored successfully.")
            delete_old_data()
            print("Old data deleted successfully.")
        else:
            print("No data to store.")
        time.sleep(60) # Wait for 60 seconds before the next iteration

if __name__ == "__main__":
    main()
