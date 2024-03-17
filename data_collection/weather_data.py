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

from tqdm import tqdm
import pandas as pd

def fetch_and_merge_weather_data(all_data_df, fetch_weather_data):
    """
    Fetches weather data for each row in the input DataFrame and merges it back.

    Parameters:
    - all_data_df (pd.DataFrame): DataFrame containing the game data with latitude, longitude, and date.
    - fetch_weather_data (function): Function to fetch weather data given latitude, longitude, and date.

    Returns:
    - pd.DataFrame: The input DataFrame merged with the fetched weather data.
    """

    weather_data_list = []

    # Iterate over the DataFrame rows with a progress bar
    for index, row in tqdm(all_data_df.iterrows(), total=all_data_df.shape[0]):
        weather_data = fetch_weather_data(row['lat'], row['lon'], row['Date'])
        print(weather_data)
        weather_data['game_id'] = row['game_id']
        weather_data_list.append(weather_data)

    # Convert the list of weather data to a DataFrame
    weather_data_df = pd.DataFrame(weather_data_list)

    # Merge the weather data with the original DataFrame
    merged_df = (
        pd
        .merge(all_data_df, weather_data_df, on='game_id')
        .assign(avg_temp=lambda df_: (df_['temperature_2m_max'] + df_['temperature_2m_min']) / 2,
                Date=pd.to_datetime(all_data_df['Date'], dayfirst=True))
    )

    return merged_df

# url = " https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{name}/{date}/{date}?unitGroup=metric&key=ZMM2U9XUSJ6UV37L4L49NQACY&options=preview&contentType=json"

# headers = {
#     'Accept': 'application/json',
#     'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
#     "referer": "https://www.visualcrossing.com/weather-data"
# }

# r = requests.get(url, headers=headers)
# print(r.json())

def fetch_weather_data(name, date, api_key="ZMM2U9XUSJ6UV37L4L49NQACY"):
    formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{name}/{formatted_date}/{formatted_date}?unitGroup=metric&key={api_key}&options=preview&contentType=json"

    headers = {
        'Accept': 'application/json',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
        "referer": "https://www.visualcrossing.com/weather-data" 
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        sleep(0.5)  # Respect the service's rate limit
        data = response.json()
        hours_data = data.get('days', [{}])[0].get('hours', [])
        # print(hours_data)
        midday_data = next((hour for hour in hours_data if hour['datetime'].endswith('13:00:00')), None)
        if midday_data:
            # Extract the relevant fields from the midday data
            weather_data = {
                'temp': midday_data.get('temp'),
                'feelslike': midday_data.get('feelslike'),
                'humidity': midday_data.get('humidity'),
                'dew': midday_data.get('dew'),
                'precip': midday_data.get('precip'),
                'windspeed': midday_data.get('windspeed'),
                'winddir': midday_data.get('winddir'),
                'pressure': midday_data.get('pressure'),
                'visibility': midday_data.get('visibility'),
                'cloudcover': midday_data.get('cloudcover'),
                'solarradiation': midday_data.get('solarradiation'),
                'conditions': midday_data.get('conditions'),
            }
            return weather_data
    return None


def fetch_and_merge_weather_data(all_data_df, fetch_weather_data):
    weather_data_list = []

    for index, row in tqdm(all_data_df.iterrows(), total=all_data_df.shape[0]):
        weather_data = fetch_weather_data(row['city_name'], row['Date'])  
        if weather_data:
            print(f"fetched weather data for {row['city_name']} on {row['Date']}")
            weather_data['game_id'] = row['game_id']
            print(weather_data)
            weather_data_list.append(weather_data)
        else:
            print(f"weather data not found for {row['city_name']} on {row['Date']}")

    # Convert the list of weather data to a DataFrame
    weather_data_df = pd.DataFrame(weather_data_list)

    # Merge the weather data with the original DataFrame
    merged_df = pd.merge(all_data_df, weather_data_df, on='game_id')

    return merged_df

merged_df = fetch_and_merge_weather_data(all_data_df, fetch_weather_data)
merged_df.to_csv("/Users/admin/git_projects/football/data_collection/serie_a_weather.csv", index=False)