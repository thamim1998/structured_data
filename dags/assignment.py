import datetime
import json
from dataclasses import dataclass
from datetime import datetime

from airflow.decorators import dag, task


import requests


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 11),
    catchup=False
)
def assignment():

    @task
    def get_departed_flights():
        api = 'https://opensky-network.org/api/flights/departure'
        params = {
            'airport': 'EDDF',
            'begin': 1688478878,
            'end': 1688565278
        }

        response = requests.get(api, params=params)
        if response.status_code == 200:
            print(response.json())
            return response.json()
        else:
            print('Error:', response.status_code)
            return None

    @task
    def export_flights_to_json(flight_string):
        file='./dags/flights.json'
        try:
            print("Flight details received:")
            print(flight_string)
            with open(file, 'w') as file:
                json.dump(flight_string, file, indent=4)
            print("Flight details written to '{file}' successfully.")
        except Exception as e:
            print('Error:', str(e))

    response_text = get_departed_flights()
    if response_text:
        export_flights_to_json(response_text)

_ = assignment()