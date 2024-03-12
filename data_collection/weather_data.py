import sys
import os
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import requests
import certifi
import ssl

from assets.cities import cities

import geopy.geocoders
from geopy.geocoders import Nominatim

from time import sleep
import pandas as pd


# r = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=51.5&longitude=0.127&start_date=2009-01-01&end_date=2009-01-02&hourly=temperature_2m,rain,wind_speed_10m,soil_moisture_0_to_7cm")
# # print(r.json())

# location = geo.geocode("Firenze").raw
def get_coordinates(cities_dict: dict):
    ctx = ssl.create_default_context(cafile=certifi.where())
    geopy.geocoders.options.default_ssl_context = ctx
    geo = Nominatim(user_agent="football-weather-data")
    for key, value in cities_dict.items():
        print(value)
        location = geo.geocode(value, timeout=10)
        if location:
            # Update the dictionary with latitude and longitude
            cities_dict[key] = {
                'name': value,
                'lat': location.latitude,
                'lon': location.longitude
            }
        else:
            print(f"Location not found for {value}")
        sleep(1.5)  # service's rate limit

    return cities_dict

df = pd.read_csv("/Users/admin/git_projects/football/data_collection/serie_a.csv")


        

r = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=38.103539&longitude=15.639756&start_date=2009-01-01&end_date=2009-01-01&timezone=Europe%2FBerlin&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max")
print(r.json())
print(pd.DataFrame(r.json()))
