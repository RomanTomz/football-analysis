import os
import sys
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import pandas as pd
import numpy as np

import plotly.graph_objects as go

from data_collection.data_collector import DataCollector



import pandas as pd
import plotly.graph_objects as go

class MatchHistory:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def one_to_one(self, home_team: str, away_team: str, total: bool = False):
        if total:
            head_to_head = self.df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}') or (HomeTeam == '{away_team}' and AwayTeam == '{home_team}')")
        else:
            head_to_head = self.df.query(f"(HomeTeam == '{home_team}' and AwayTeam == '{away_team}')")
        return head_to_head.copy()

    def match_stats(self, head_to_head: pd.DataFrame, home_team: str, away_team: str):
        if head_to_head.empty:
            return pd.DataFrame()

        # Directly modify the copy
        head_to_head['Result'] = np.select(
            [
                (head_to_head['HomeTeam'] == home_team) & (head_to_head['FTR'] == 'H'),
                (head_to_head['AwayTeam'] == home_team) & (head_to_head['FTR'] == 'A'),
                (head_to_head['AwayTeam'] == away_team) & (head_to_head['FTR'] == 'A'),
                (head_to_head['HomeTeam'] == away_team) & (head_to_head['FTR'] == 'H'),
                (head_to_head['FTR'] == 'D')
            ], 
            ['Home Wins', 'Home Wins', 'Away Wins', 'Away Wins', 'Draws'],
            default='Draw'
        )

        stats = head_to_head['Result'].value_counts().rename_axis('Result').reset_index(name='Counts')
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
    


