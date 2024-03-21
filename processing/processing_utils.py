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
        self.home_team = None
        self.away_team = None
    
    def one_to_one(self, home_team: str, away_team:str, total: bool = False):
        """
        Returns a DataFrame containing the one-to-one match history between two teams.

        Parameters:
        home_team (str): The name of the home team.
        away_team (str): The name of the away team.

        Returns:
        DataFrame: A DataFrame containing the one-to-one match history between the two teams.
        """
        self.home_team = home_team
        self.away_team = away_team
        
        if total:
            self.head_to_head = self.df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}') or (HomeTeam == '{away_team}' and AwayTeam == '{home_team}')")
        else:
            self.head_to_head = self.df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}')")
            
        return self.head_to_head
    
    def match_stats(self):
        """
        Returns the match statistics for a given match DataFrame.

        Parameters:
        match_df (DataFrame): The DataFrame containing the match data.

        Returns:
        dict: A dictionary containing the match statistics.
        """
        if self.head_to_head is None or self.head_to_head.empty:
            raise ValueError("No head-to-head data. Please run one_to_one method first.")

        home_wins = len(self.head_to_head.query(f"(HomeTeam == '{self.home_team}' and AwayTeam == '{self.away_team}' and FTR == 'H') or (HomeTeam == '{self.away_team}' and AwayTeam == '{self.home_team}' and FTR == 'A')"))
        away_wins = len(self.head_to_head.query(f"(AwayTeam == '{self.away_team}' and HomeTeam == '{self.home_team}' and FTR == 'A') or (AwayTeam == '{self.home_team}' and HomeTeam == '{self.away_team}' and FTR == 'H')"))
        draws = len(self.head_to_head.query(f"(HomeTeam == '{self.home_team}' and AwayTeam == '{self.away_team}' and FTR == 'D') or (HomeTeam == '{self.away_team}' and AwayTeam == '{self.home_team}' and FTR == 'D')"))

        # Calculate the total goals
        home_goals = self.head_to_head.query(f"HomeTeam == '{self.home_team}'")['FTHG'].sum() + self.head_to_head.query(f"AwayTeam == '{self.home_team}'")['FTAG'].sum()
        away_goals = self.head_to_head.query(f"AwayTeam == '{self.away_team}'")['FTAG'].sum() + self.head_to_head.query(f"HomeTeam == '{self.away_team}'")['FTHG'].sum()

        stats = pd.DataFrame({
            'Total Matches': [len(self.head_to_head)],
            'Home Wins': [home_wins],
            'Away Wins': [away_wins],
            'Draws': [draws],
            'Home Goals': [home_goals],
            'Away Goals': [away_goals]
        })

        return stats
    
history = MatchHistory(path)
print(match_df := history.one_to_one("Reggina", "Inter", total=True))  
stats = history.match_stats()  
print(stats)
