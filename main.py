import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import json
import uuid
import psycopg2
from psycopg2 import sql


from config import config

urls = [
    'https://www.meteored.mx/ciudad-de-mexico/historico',
    'https://www.meteored.mx/monterrey/historico',
    'https://www.meteored.mx/merida/historico',
    'https://www.meteored.mx/wakanda/historico'
]

def write_to_json(new_data):
    file_path = "data.json"

    # Load existing data from the JSON file, if any
    try:
        with open(file_path, "r") as file:
            existing_data = json.load(file)
    except FileNotFoundError:
        existing_data = []

    # Append the new data to the existing data
    existing_data.append(new_data)

    # Write the updated data back to the JSON file
    with open(file_path, "w") as file:
        json.dump(existing_data, file)


async def send_request(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            status = response.status
            if status == 200:
                html_content = await response.text()
                # regex para encontrar la fecha de ultima actualizacion
                date_pattern = r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
                date = re.search(date_pattern, html_content).group()
                
                # BeatifulSoup para encontrar los demas campos usando 'span_id'
                soup = BeautifulSoup(html_content, 'html.parser')
                distance = soup.find('span', id='dist_cant').text
                temperature = soup.find('span', id='ult_dato_temp').text
                humidity = soup.find('span', id='ult_dato_hum').text

                data = {
                    "id": str(uuid.uuid4()),
                    "url": url,
                    "status": status,
                    "date" : date,
                    "distance" : distance,
                    "temperature" : temperature,
                    "humidity" : humidity
                }
            else:
                data = {
                    "id": str(uuid.uuid4()),
                    "url": url,
                    "status": status,
                }

    write_to_json(data)



def create_db():
    """ Create the DataBase in PostgreSQL """
    connection = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)

        # Create a cursor
        cursor = connection.cursor()

        # Create a catalog/schema
        catalog_name = 'cities_catalog'

        create_catalog_query = f"CREATE SCHEMA IF NOT EXISTS {catalog_name}"
        cursor.execute(create_catalog_query)

        # Create a table for cities if it doesn't exist
        cities_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.cities (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )
        """).format(sql.Identifier(catalog_name))
        cursor.execute(cities_table_query)

        # Create a table for HTTP code responses if it doesn't exist
        http_responses_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.http_responses (
                id SERIAL PRIMARY KEY,
                url VARCHAR(100) NOT NULL,
                code INTEGER NOT NULL
            )
        """).format(sql.Identifier(catalog_name))
        cursor.execute(http_responses_table_query)

        # Create a table for retrieved data if it doesn't exist
        retrieved_data_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.retrieved_data (
                id SERIAL PRIMARY KEY,
                url VARCHAR(100) NOT NULL,
                status VARCHAR(100) NOT NULL,
                date VARCHAR(100) NOT NULL,
                distance VARCHAR(100) NOT NULL,
                temperature VARCHAR(100) NOT NULL,
                humidity VARCHAR(100) NOT NULL
            )
        """).format(sql.Identifier(catalog_name), sql.Identifier(catalog_name), sql.Identifier(catalog_name))
        cursor.execute(retrieved_data_table_query)

        # Commit the changes
        connection.commit()

        # Close the cursor and connection
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


# Function to insert data
def insert_data(city_name, http_code, http_description, retrieved_data):
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)

        # Create a cursor
        cursor = connection.cursor()



        # Insert city into cities table
        insert_city_query = """
            INSERT INTO your_catalog.cities (name)
            VALUES (%s)
            RETURNING id
        """
        cursor.execute(insert_city_query, (city_name,))
        city_id = cursor.fetchone()[0]
        
        # Insert HTTP code response into http_responses table
        insert_http_response_query = """
            INSERT INTO your_catalog.http_responses (code, description)
            VALUES (%s, %s)
            RETURNING id
        """
        cursor.execute(insert_http_response_query, (http_code, http_description))
        http_response_id = cursor.fetchone()[0]
        
        # Insert retrieved data into retrieved_data table
        insert_retrieved_data_query = """
            INSERT INTO your_catalog.retrieved_data (city_id, http_response_id, data)
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_retrieved_data_query, (city_id, http_response_id, retrieved_data))
        
        # Commit the changes
        connection.commit()
        
        print("Data inserted successfully!")
        
    except (Exception, psycopg2.Error) as error:
        print("Error inserting data:", error)
        
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()



async def main():
    tasks = []
    for url in urls:
        task = asyncio.create_task(send_request(url))
        tasks.append(task)
    
    await asyncio.gather(*tasks)



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    create_db()