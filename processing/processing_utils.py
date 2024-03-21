import os
import sys
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import pandas as pd

import plotly.graph_objects as go

from data_collection.data_collector import DataCollector



class MatchHistory:
    def __init__(self, df: pd.DataFrame):
        
        self.df = df
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
    
    def head_to_head_viz(self):
        
        fig = go.Figure()
        
        fig.add_trace(
            go.Bar(
                x=self.head_to_head['FTR'].value_counts().index,
                y=self.head_to_head['FTR'].value_counts().values,
            )
        )
        
        fig.update_layout(
            title=f"{self.home_team} vs {self.away_team} Head-to-Head",
            xaxis_title="Result",
            yaxis_title="Count",
            barmode="group"
        )
        
        fig.show()
        

if __name__ == "__main__":
    collector = DataCollector('serie_a')
    df = collector.collect_data(2003, 2023)
    match_history = MatchHistory(df)
    match = match_history.one_to_one('Juventus', 'Milan', total=True)
    print(match)
    stats = match_history.match_stats()
    print(stats)
    fig = match_history.head_to_head_viz()
    fig.show()
    


