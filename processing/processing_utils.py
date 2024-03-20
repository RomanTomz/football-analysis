import os
import sys
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import pandas as pd

from data_collection.data_reader import collect_data

df = pd.read_csv("/Users/admin/git_projects/football/data_collection/serie_a_weather_2.csv")

def one_to_one(df, home_team: str, away_team:str):
    """
    Returns a DataFrame containing the one-to-one match history between two teams.

    Parameters:
    df (DataFrame): The input DataFrame containing the match history.
    home_team (str): The name of the home team.
    away_team (str): The name of the away team.

    Returns:
    DataFrame: A DataFrame containing the one-to-one match history between the two teams.
    """
    return df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}')")

print(one_to_one(df, "Juventus", "Inter"))