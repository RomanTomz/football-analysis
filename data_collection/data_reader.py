import sys
import os
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import pandas as pd
import requests
from io import StringIO
import os
import uuid

from assets.cities import cities

all_data = []
def collect_data(year_start, year_end, write_csv=False, league='serie_a'):
    """
    Collects football match data for a specified range of seasons and league, 
    and optionally writes the data to a CSV file.

    This function fetches match data from football-data.co.uk for the specified 
    league and range of seasons. It supports Serie A and English Premier League data.
    The data is concatenated into a single DataFrame. If `write_csv` is True, 
    the data is also saved to a CSV file in the specified directory.

    Parameters:
    year_start (int): The starting year of the season range (inclusive).
    year_end (int): The ending year of the season range (inclusive).
    write_csv (bool, optional): If True, writes the collected data to a CSV file. Defaults to False.
    league (str, optional): The league to collect data for. Can be 'serie_a' or 'epl'. Defaults to 'serie_a'.

    Returns:
    pandas.DataFrame: A DataFrame containing the concatenated match data for all requested seasons.

    Side Effects:
    - Prints messages to stdout regarding the status of data fetching.
    - If write_csv is True, a CSV file is created in the 'data_collection' directory.
    """
    for year in range(year_start, year_end + 1):
        if league == 'serie_a':
            url = f"https://www.football-data.co.uk/mmz4281/{str(year)[-2:]}{str(year+1)[-2:]}/I1.csv"
        elif league == 'epl':
            url = f"https://www.football-data.co.uk/mmz4281/{str(year)[-2:]}{str(year+1)[-2:]}/E0.csv"
        else:
            raise ValueError("Invalid league. Must be 'serie_a' or 'epl'.")
        
        try:
            r = requests.get(url)
            if r.status_code == 200:
                data = StringIO(r.text)
                
                df = pd.read_csv(data, on_bad_lines='skip')
                
                all_data.append(df)
            else:
                print(f"Data for season {year}/{year+1} not found or could not be retrieved.")
        except requests.RequestException as e:
            print(f"Request failed for season {year}/{year+1}: {e}")

    all_data_df = pd.concat(all_data, ignore_index=True)
    all_data_df = (
        all_data_df
        .assign(Div = league,
                Date = pd.to_datetime(all_data_df['Date'], dayfirst=True),
                game_id = [uuid.uuid4().hex[:8] for _ in range(len(all_data_df))],
                TG = all_data_df['FTHG'] + all_data_df['FTAG'],
                city_name = all_data_df['HomeTeam'].map(lambda x: cities[x]['name'] if x in cities else None),
                lat = all_data_df['HomeTeam'].map(lambda x: cities[x]['lat'] if x in cities else None),
                lon = all_data_df['HomeTeam'].map(lambda x: cities[x]['lon'] if x in cities else None))
        # .loc[:, ['game_id'] + [col for col in all_data_df.columns if col != 'game_id'][:3] + ['TG'] + [col for col in all_data_df.columns if col != 'game_id'][4:] + [col for col in all_data_df.columns if col != 'TG']]
        .dropna(how='all', axis=1)
    )
    cols = ['game_id'] + [col for col in all_data_df.columns if col not in ['game_id', 'TG']][:3] + ['TG'] + [col for col in all_data_df.columns if col not in ['game_id', 'TG', 'TG']][3:]

    all_data_df = all_data_df[cols]
    
    if write_csv:
        output_dir = 'data_collection'
        os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists before writing
        output_file = f'{output_dir}/{league}.csv'
        all_data_df.to_csv(output_file, index=False)
    return all_data_df

# Example usage
collect_data(2003, 2020, write_csv=True, league='serie_a')
