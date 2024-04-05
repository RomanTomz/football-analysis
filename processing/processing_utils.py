import os
import sys
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import pandas as pd
import numpy as np

import sqlite3

from data_collection.data_collector import DataCollector


class MatchHistory:
    def __init__(self, league, df):
        self.df = df
        self.league = league
        self.db_path = os.path.join(root_path, 'data_collection', 'data', f'{league}.db')
        
    def get_teams(self, league, year_start, year_end):
        query = f"SELECT DISTINCT HomeTeam FROM {league}_data WHERE season>='{year_start}' AND season <= '{year_end}'"
        with sqlite3.connect(self.db_path) as conn:
            teams = pd.read_sql_query(query, conn)
        return teams['HomeTeam'].sort_values().tolist()
    
    def fetch_league_data(self, league, year_start, year_end):
        query = f"SELECT * FROM {league}_data WHERE season >= '{year_start}' AND season <= '{year_end}'"
        with sqlite3.connect(self.db_path) as conn:
            data = pd.read_sql_query(query, conn)
        return data
# TODO: modify fetch_head_to_head_data to respond to year_start and year_end dynamically
    def fetch_head_to_head_data(self, home_team: str, away_team: str, year_start: int, year_end: int, total: bool = False):
        with sqlite3.connect(self.db_path) as conn:
            if total:
                query = f"""
                SELECT * FROM {self.league}_data 
                WHERE 
                ((HomeTeam = '{home_team}' AND AwayTeam = '{away_team}') 
                OR (HomeTeam = '{away_team}' AND AwayTeam = '{home_team}'))
                AND season >= '{year_start}' AND season <= '{year_end}'
                """
            else:
                query = f"""
                SELECT * FROM {self.league}_data 
                WHERE 
                HomeTeam = '{home_team}' AND AwayTeam = '{away_team}'
                AND season >= '{year_start}' AND season <= '{year_end}'
                """
            head_to_head = pd.read_sql_query(query, conn)
        return head_to_head

# TODO: modify match_stats to query the database directly
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

        

if __name__ == "__main__":
    collector = DataCollector('serie_a')
    df = collector.collect_data(2003, 2023)
    match_history = MatchHistory(df)


    


