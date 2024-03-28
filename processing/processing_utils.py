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
        conn = sqlite3.connect(self.db_path)
        teams = pd.read_sql_query(f"SELECT DISTINCT HomeTeam FROM {league}_data WHERE season>='{year_start}' AND season <= '{year_end}'", conn)
        conn.close()
        return teams['HomeTeam'].sort_values().tolist()
    
    def fetch_league_data(self, league, year_start, year_end):
        conn = sqlite3.connect(self.db_path)
        query = f"SELECT * FROM {league}_data WHERE season >= '{year_start}' AND season <= '{year_end}'"
        data = pd.read_sql_query(query, conn)
        conn.close()
        return data

    def fetch_head_to_head_data(self, home_team: str, away_team: str, total: bool = False):
        table_name = f"{self.league}_data"  # Dynamic table name based on the league
        try:
            with sqlite3.connect(self.db_path) as conn:
                if total:
                    query = f"SELECT * FROM {table_name} WHERE ((HomeTeam = '{home_team}' AND AwayTeam = '{away_team}') OR (HomeTeam = '{away_team}' AND AwayTeam = '{home_team}'))"
                else:
                    query = f"SELECT * FROM {table_name} WHERE HomeTeam = '{home_team}' AND AwayTeam = '{away_team}'"
                head_to_head = pd.read_sql_query(query, conn)
            return head_to_head
        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame() 

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


    


