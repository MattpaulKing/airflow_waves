import json
import pandas as pd
from typing import TypedDict
import arrow
import requests as r
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from airflow.decorators import dag, task

Coords = TypedDict("Coords", {"beach": str, "lat": float, "lng": float })


@dag(
    schedule="@daily",
    tags=["waves"]

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
            }, {
                "beach": "jordan river",
                "lat": 48.50033,
                "lng": -124.30093,
        }]
        responses = []
        start = arrow.now().floor('day')
        end = arrow.now().ceil('day')
        for coords in beach_coords:
            for i in range(5):
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
                        'start': start.shift(days=i).to('UTC').timestamp(),  # Convert to UTC timestamp
                        'end': end.shift(days=i).to('UTC').timestamp()  # Convert to UTC timestamp
                    },
                    headers={
                        'Authorization': api_key
                    }
                )
                waves_data = response.json()
                waves_data['beach'] = coords['beach']
                responses.append(waves_data)
           
        return responses

    @task()
    def transform(waves_data: list):
        hour_records = []
        for idx, beach in enumerate(waves_data):
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
        return res

    @task()
    def load(waves_df: pd.DataFrame, db_conn_str: str):
        """

        TODO: DOCS


        """
        engine = create_engine(db_conn_str, echo=False)
        conn = engine.connect()
        _ = waves_df.to_sql("waves", conn, if_exists="append", index=False)

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
