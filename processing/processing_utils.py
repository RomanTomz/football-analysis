import os
import sys
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import pandas as pd

from data_collection.data_reader import collect_data


path = "/Users/admin/git_projects/football/data_collection/serie_a.csv"

def one_to_one(df, home_team: str, away_team:str, total: bool = False):
    """
    Returns a DataFrame containing the one-to-one match history between two teams.

    Parameters:
    df (DataFrame): The input DataFrame containing the match history.
    home_team (str): The name of the home team.
    away_team (str): The name of the away team.

    Returns:
    DataFrame: A DataFrame containing the one-to-one match history between the two teams.
    """
    if total:
        return df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}') or (HomeTeam == '{away_team}' and AwayTeam == '{home_team}')")
    else:
        return df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}')")

# print(one_to_one(df, "Juventus", "Inter", total=True))

class MatchHistory:
    def __init__(self, data_path: str):
        
        self.df = pd.read_csv(data_path)
    
    def one_to_one(self, home_team: str, away_team:str, total: bool = False):
        """
        Returns a DataFrame containing the one-to-one match history between two teams.

        Parameters:
        home_team (str): The name of the home team.
        away_team (str): The name of the away team.

        Returns:
        DataFrame: A DataFrame containing the one-to-one match history between the two teams.
        """
        if total:
            return self.df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}') or (HomeTeam == '{away_team}' and AwayTeam == '{home_team}')")
        else:
            return self.df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}')")
    
    def match_stats(self, match_df):
        """
        Returns the match statistics for a given match DataFrame.

        Parameters:
        match_df (DataFrame): The DataFrame containing the match data.

        Returns:
        dict: A dictionary containing the match statistics.
        """
        stats = {
            "home_goals": match_df["FTHG"].sum(),
            "away_goals": match_df["FTAG"].sum(),
        }
        
        return stats
    
history = MatchHistory(path)
print(match_df := history.one_to_one("Juventus", "Inter", total=True))
stats = history.match_stats(match_df)
print(stats)