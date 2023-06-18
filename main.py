import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import json

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
                    "url": url,
                    "status": status,
                    "date" : date,
                    "distance" : distance,
                    "temperature" : temperature,
                    "humidity" : humidity
                }
            else:
                data = {
                    "url": url,
                    "status": status,
                }

    write_to_json(data)


async def main():
    tasks = []
    for url in urls:
        task = asyncio.create_task(send_request(url))
        tasks.append(task)
    
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())