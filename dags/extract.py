import datetime
from typing import TypedDict
import arrow
import requests as r
from dotenv import load_dotenv
import os

Coords = TypedDict("Coords", {"lat": float, "lng": float })


def waves_etl():
    """

        TODO: DOCS

    """
    def extract(coords: Coords):
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
        load_dotenv()
        start = arrow.now().floor('day')
        end = arrow.now().ceil('day')

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
                'end': end.to('UTC').timestamp()  # Convert to UTC timestamp
            },
            headers={
                'Authorization': os.environ.get("STORMGLASS_API_KEY")
            }
        )
        res = response.json()
        print(res)
        return res['hours']

    def transform(waves_data: list):
        """
        
        TODO: Must get average for every value

        """
        for beach in waves_data:
            for hour_record in beach:
                for key in hour_record:
                    print(key)
                    prop_value = 0
                    count = 0
                    for source in hour_record[key]:
                        prop_value += hour_record[key][source]
                        count += 1
                    prop_value = prop_value / count
                    hour_record[key] = prop_value
        return waves_data

    def load(waves_data):
        """

        TODO: DOCS


        """

    beach_coords: dict[str, Coords] = {
        "sombrio": {
            "lat": 48.50033,
            "lng": -124.30093,
        },
        "jordan_river": {
            "lat": 0,
            "lng": 0,
        }
    }

    sombrio_data = extract(beach_coords["sombrio"])
    jordan_river_data= extract(beach_coords["jordan_river"])

    waves_data = transform([sombrio_data, jordan_river_data])
    load(waves_data)

waves_etl()
