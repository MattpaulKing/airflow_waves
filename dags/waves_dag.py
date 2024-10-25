import json
from airflow.models.variable import Variable
import pandas as pd
from typing import TypedDict
import arrow
import requests as r
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from airflow.decorators import dag, task
from datetime import datetime

Coords = TypedDict("Coords", {"beach": str, "lat": float, "lng": float })


@dag(
    schedule="@daily",
    tags=["waves"],
    catchup=False,
    start_date=datetime.now(),
    default_args={
        "email": ["mking@coolaid.org"]
    }
)
def waves_etl():
    @task()
    def extract():

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

        api_key = Variable.get("STORMGLASS_API_KEY")
        db_conn_uri = Variable.get("DB_CONN_URI")
        if not api_key or not db_conn_uri:
            return 1

        beach_coords: list[Coords] = [{
                "beach": "sombrio",
                "lat": 48.50033,
                "lng": -124.30093,
        }]
        waves_data = []
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
            record = response.json()
            record['beach'] = coords['beach']

            if "errors" not in record:
                waves_data.append(record)

        with open("waves.json", "w") as outfile:
            outfile.write(json.dumps(waves_data, indent=4))

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
        waves_rows = res.to_dict(orient='records')

        metadata = MetaData()
        engine = create_engine(db_conn_uri, echo=False)
        waves_table = Table("waves", metadata, autoload_with=engine)
        with engine.connect() as conn:
            conn.execute(
                insert(waves_table),
                waves_rows
            )
        return 0

    _ = extract()


waves_etl()
