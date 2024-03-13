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
from tqdm import tqdm
import pandas as pd


# r = requests.get("https://archive-api.open-meteo.com/v1/archive?latitude=51.5&longitude=0.127&start_date=2009-01-01&end_date=2009-01-02&hourly=temperature_2m,rain,wind_speed_10m,soil_moisture_0_to_7cm")
# # print(r.json())

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

all_data_df = pd.read_csv("/Users/admin/git_projects/football/data_collection/serie_a.csv")
all_data_df = all_data_df.dropna(subset=['Date'])
        

def fetch_weather_data(lat, lon, date):
    formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={formatted_date}&end_date={formatted_date}&timezone=Europe%2FBerlin&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
    
    response = requests.get(url)
    print(response)
    if response.status_code == 200:
        sleep(0.5)  # service's rate limit
        data = response.json()
        daily_data = data.get('daily', {})
        weather_data = {
            'temperature_2m_max': daily_data.get('temperature_2m_max', [None])[0],
            'temperature_2m_min': daily_data.get('temperature_2m_min', [None])[0],
            'precipitation_sum': daily_data.get('precipitation_sum', [None])[0],
            'wind_speed_10m_max': daily_data.get('wind_speed_10m_max', [None])[0],
        }
        return weather_data
    else:
        return {'temperature_2m_max': None, 'temperature_2m_min': None, 'precipitation_sum': None, 'wind_speed_10m_max': None}

weather_data_list = []

# Wrap the iteration with tqdm for a progress bar
for index, row in tqdm(all_data_df.iterrows(), total=all_data_df.shape[0]):
    weather_data = fetch_weather_data(row['lat'], row['lon'], row['Date'])
    weather_data['game_id'] = row['game_id']
    weather_data_list.append(weather_data)

weather_data_df = pd.DataFrame(weather_data_list)
all_data_df = pd.merge(all_data_df, weather_data_df, on='game_id')

print(weather_data_df)

all_data_df.to_csv("/Users/admin/git_projects/football/data_collection/serie_a_weather.csv", index=False)
