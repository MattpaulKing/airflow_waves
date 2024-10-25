import json
import pandas as pd
from typing import TypedDict
import arrow
import requests as r
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
from airflow.decorators import dag, task
from datetime import datetime

Coords = TypedDict("Coords", {"beach": str, "lat": float, "lng": float })


@dag(
    schedule="@daily",
    tags=["waves"],
    catchup=False,
    start_date=datetime.now()
)
def waves_etl():
    @task()
    def extract(api_key: str):
        """
        Fetches data from Storglass.io 

        Returns:
        [{ 
            "time": "%UTC_TIMESTAMP%, 
            %PARAM_KEY%: {
                %SOURCE%: %PARAM_VALUE%
            }
        }]
        """

        beach_coords: list[Coords] = [{
                "beach": "sombrio",
                "lat": 48.50033,
                "lng": -124.30093,
        }]
        #     }, {
        #         "beach": "jordan river",
        #         "lat": 48.50033,
        #         "lng": -124.30093,
        # }]
        responses = []
        start = arrow.now().floor('day')
        end = arrow.now().ceil('day')
        for coords in beach_coords:
            response = r.get(
                'https://api.stormglass.io/v2/weather/point',
                params={
                    'lat': coords["lat"],
                    'lng': coords["lng"],
                    'params': ','.join([
                        'waveHeight',
                        'airTemperature',
                        'swellDirection',
                        'swellHeight',
                        'swellPeriod',
                        'secondarySwellDirection',
                        'secondarySwellHeight',
                        'secondarySwellPeriod',
                        'waveDirection',
                        'waveHeight',
                        'wavePeriod',
                        'windWaveDirection',
                        'windWaveHeight',
                        'windWavePeriod',
                        'windDirection',
                        'windSpeed'
                    ]),
                    'start': start.to('UTC').timestamp(),  # Convert to UTC timestamp
                    'end': end.shift(days=5).to('UTC').timestamp()  # Convert to UTC timestamp
                },
                headers={
                    'Authorization': api_key
                }
            )
            waves_data = response.json()
            waves_data['beach'] = coords['beach']

            if "errors" not in waves_data:
                responses.append(waves_data)

        with open("waves.json", "w") as outfile:
            outfile.write(json.dumps(responses, indent=4))
            

        return responses

    @task()
    def transform(waves_data: list):
        hour_records = []
        for idx, beach in enumerate(waves_data):
            if 'errors' in beach:
                continue
            for hour_record in beach['hours']:
                hour_record['beach'] = beach['beach']
                for key in hour_record:
                    if key == "time" or key == "beach":
                        continue
                    prop_value = 0
                    count = 0
                    for source in hour_record[key]:
                        prop_value += hour_record[key][source]
                        count += 1
                    prop_value = prop_value / count
                    hour_record[key] = prop_value
                hour_records.append(hour_record)

        res = pd.DataFrame.from_records(hour_records).rename(columns={"time": "waveTs"})
        res = res.to_dict(orient='records')
        return res

    @task()
    def load(waves_rows: list[dict], db_conn_str: str):
        """

        TODO: DOCS


        """
        metadata = MetaData()
        engine = create_engine(db_conn_str, echo=False)
        waves_table = Table("waves", metadata, autoload_with=engine)
        with engine.connect() as conn:
            result = conn.execute(
                insert(waves_table),
                waves_rows
            )
        return 1
    

    load_dotenv()
    db_conn_str = os.environ.get("DB_CONN")
    api_key = os.environ.get("STORMGLASS_API_KEY")
    if not db_conn_str or not api_key:
        return -1

    responses = extract(api_key)
    waves_data = transform(responses)
    load(waves_data, db_conn_str)


waves_etl()
