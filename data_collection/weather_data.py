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
        


def fetch_weather_data(name, date, api_key="ZMM2U9XUSJ6UV37L4L49NQACY"):
    """
    Fetches weather data for a specific location and date.

    Args:
        name (str): The name of the location.
        date (str): The date for which weather data is requested (format: 'YYYY-MM-DD').
        api_key (str, optional): The API key for accessing the weather data service. Defaults to "ZMM2U9XUSJ6UV37L4L49NQACY".

    Returns:
        dict or None: A dictionary containing the weather data for the specified location and date, or None if the data is not available.
    """
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
    """
    Fetches weather data for each row in the given DataFrame and merges it with the original DataFrame.

    Args:
        all_data_df (pandas.DataFrame): The original DataFrame containing game data.
        fetch_weather_data (function): A function that fetches weather data for a given city and date.

    Returns:
        pandas.DataFrame: The merged DataFrame containing the original game data and the fetched weather data.
    """
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