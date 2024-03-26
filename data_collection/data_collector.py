import pandas as pd
import requests
from io import StringIO
import uuid

import sqlite3
from sqlite3 import Error

from tqdm import tqdm
from time import sleep

import sys
import os

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from assets.cities import cities


class DataCollector:
    """
    A class for collecting football data from the https://www.football-data.co.uk.

    Attributes:
        league (str): The league for which data is collected ('serie_a' or 'epl').
        all_data (list): A list to store the collected data.

    Methods:
        collect_data(year_start, year_end, write_csv=False): Collects data for the specified years.
        _construct_url(year): Constructs the URL for the data based on the league and year.
        _process_data(write_csv): Processes the collected data and returns a DataFrame.
    """

    def __init__(self, league: str, db_path=None):
        """
        Initializes a DataCollector object.

        Args:
            league (str): The league for which data is collected ('serie_a' or 'epl').
        """
        self.league = league
        self.all_data = []
        self.db_path = os.path.join(root_path, "data_collection", "data", f"{league}.db")
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        
    def create_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            print(f"Connection to {self.db_path} successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        
        return conn

    def collect_data(self, year_start: int, year_end: int, write_csv=False):
        """
        Collects data for the specified years.

        Args:
            year_start (int): The starting year.
            year_end (int): The ending year.
            write_csv (bool, optional): Whether to write the collected data to a CSV file. Defaults to False.

        Returns:
            pandas.DataFrame: The processed data as a DataFrame.
        """
        max_attempts = 3

        for year in tqdm(range(year_start, year_end + 1), desc="Fetching data"):
            success = False
            attempt = 0

            while attempt < max_attempts and not success:
                try:
                    url = self._construct_url(year)
                    r = requests.get(url, headers=self.headers)
                    # sleep(0.5)  
                    if r.status_code == 200:
                        data = StringIO(r.text)
                        df = pd.read_csv(data, on_bad_lines="skip")
                        self.all_data.append(df)
                        success = True
                    else:
                        print(
                            f"Attempt {attempt + 1}: Data for season {year}/{year + 1} not found or could not be retrieved."
                        )
                    attempt += 1
                except requests.RequestException as e:
                    print(
                        f"Attempt {attempt + 1}: Request failed for season {year}/{year + 1}: {e}"
                    )
                    attempt += 1

            if not success:
                print(
                    f"Failed to fetch data for season {year}/{year + 1} after {max_attempts} attempts."
                )

        return self._process_data(write_csv)

    def _construct_url(self, year: int):
        """
        Constructs the URL for the data based on the league and year.

        Args:
            year (int): The year for which the URL is constructed.

        Returns:
            str: The constructed URL.

        Raises:
            ValueError: If the league is invalid.
        """
        if self.league == "serie_a":
            return f"https://www.football-data.co.uk/mmz4281/{str(year)[-2:]}{str(year+1)[-2:]}/I1.csv"
        elif self.league == "epl":
            return f"https://www.football-data.co.uk/mmz4281/{str(year)[-2:]}{str(year+1)[-2:]}/E0.csv"
        else:
            raise ValueError("Invalid league. Must be 'serie_a' or 'epl'.")


    def _process_data(self, write_csv: bool) -> pd.DataFrame:
        """
        Processes the collected data and returns a DataFrame.

        Args:
            write_csv (bool): Whether to write the processed data to a CSV file.

        Returns:
            pandas.DataFrame: The processed data as a DataFrame.
        """
        if not self.all_data:
            return pd.DataFrame()  # Return an empty DataFrame if there's no data

        all_data_df = pd.concat(self.all_data, ignore_index=True)
        all_data_df = all_data_df[all_data_df["Date"].notna()]

        # Process the DataFrame
        all_data_df = all_data_df.assign(
            Div=self.league,
            Date=lambda x: pd.to_datetime(x["Date"], dayfirst=True),
            season=lambda x: [
                f"{date.year}/{str(date.year + 1)}"
                if date.month >= 8 else f"{date.year - 1}/{date.year}"
                for date in pd.to_datetime(x["Date"], dayfirst=True)
            ],
            game_id=[uuid.uuid4().hex[:8] for _ in range(len(all_data_df))],
            TG=all_data_df["FTHG"] + all_data_df["FTAG"],
            city_name=all_data_df["HomeTeam"].map(
                lambda x: cities.get(x, {}).get("name")
            ),
            lat=all_data_df["HomeTeam"].map(
                lambda x: cities.get(x, {}).get("lat")
            ),
            lon=all_data_df["HomeTeam"].map(
                lambda x: cities.get(x, {}).get("lon")
            ),
        ).dropna(how="all", axis=1)

        # Define the order of columns
        cols = (
            ["game_id"] +
            [col for col in all_data_df.columns if col not in ["game_id", "TG"]][:4] +
            ["TG"] +
            [col for col in all_data_df.columns if col not in ["game_id", "TG", "TG"]][4:]
        )

        # Reorder the DataFrame based on defined columns
        all_data_df = all_data_df[cols]

    # Optionally write to CSV
        if write_csv:
            filename = f"{self.league}.csv"
            all_data_df.to_csv(filename, index=False)
            print(f"Data written to {filename}")

        return all_data_df
    
    def write_to_db(self, df: pd.DataFrame):
        
        conn = self.create_connection()
        if conn is not None:
            df.to_sql(f'{self.league}_data', conn, if_exists='append', index=False)
            print(f"Data written to {self.league}.db")
            conn.close()
        else:
            print("Connection failed")
            
    def collect_and_update_data(self, year_start: int, year_end: int):
        """Collects and updates the data in the database."""
        new_data = self.collect_data(year_start, year_end, write_csv=False)
        
        conn = self.create_connection()
        if conn is not None:
            # Check if the table exists
            table_exists_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.league}_data';"
            if conn.execute(table_exists_query).fetchone() is None:
                # If the table doesn't exist, create it by writing the new data with if_exists='replace'
                new_data.to_sql(f'{self.league}_data', conn, if_exists='replace', index=False)
            else:
                # If the table exists, proceed to check and append new data
                existing_data = pd.read_sql_query(f"SELECT * FROM {self.league}_data", conn)
                new_data_filtered = new_data[~new_data['game_id'].isin(existing_data['game_id'])]
                if not new_data_filtered.empty:
                    self.write_to_database(new_data_filtered)
                else:
                    print("No new data to update.")
            conn.close()
        else:
            print("Error! cannot create the database connection.")
        
    @staticmethod
    def compute_team_statistics(df, year_start=None, year_end=None):
        """
        Compute team statistics based on the given DataFrame.

        Parameters:
        - df (pandas.DataFrame): The DataFrame containing the football match data.

        Returns:
        - stats (pandas.DataFrame): The computed team statistics including home and away statistics, total statistics, and various ratios.
        """
        # Aggregate home and away statistics
        # make the year_start and year_end optional
        if year_start is not None and year_end is not None:
            df_filtered = df.query(f"{year_start} <= Date.dt.year <= {year_end}")
        else:
            df_filtered = df
        home_stats = df_filtered.groupby("HomeTeam").agg(
            HomeGames=("HomeTeam", "count"),
            HomeWins=("FTR", lambda x: (x == "H").sum()),
            HomeDraws=("FTR", lambda x: (x == "D").sum()),
            HomeGoals=("FTHG", "sum"),
        )
        away_stats = df_filtered.groupby("AwayTeam").agg(
            AwayGames=("AwayTeam", "count"),
            AwayWins=("FTR", lambda x: (x == "A").sum()),
            AwayDraws=("FTR", lambda x: (x == "D").sum()),
            AwayGoals=("FTAG", "sum"),
        )

        # Merge home and away statistics
        stats = home_stats.merge(
            away_stats, left_index=True, right_index=True, how="outer"
        ).fillna(0)

        # Calculate total statistics
        stats["TotalGames"] = stats["HomeGames"] + stats["AwayGames"]
        stats["TotalWins"] = stats["HomeWins"] + stats["AwayWins"]
        stats["TotalDraws"] = stats["HomeDraws"] + stats["AwayDraws"]
        stats["TotalGoals"] = stats["HomeGoals"] + stats["AwayGoals"]

        # Calculate ratios
        stats["WinRatio"] = stats["TotalWins"] / stats["TotalGames"]
        stats["DrawRatio"] = stats["TotalDraws"] / stats["TotalGames"]
        stats["HomeWinRatio"] = stats["HomeWins"] / stats["HomeGames"]
        stats["AwayWinRatio"] = stats["AwayWins"] / stats["AwayGames"]
        stats["HomeGoalRatio"] = stats["HomeGoals"] / stats["HomeGames"]
        stats["AwayGoalRatio"] = stats["AwayGoals"] / stats["AwayGames"]
        stats["TotalGoalRatio"] = stats["TotalGoals"] / stats["TotalGames"]

        return stats.reset_index().rename(columns={"index": "Team"})


if __name__ == "__main__":
    # example usage
    dc = DataCollector(league="epl")
    # data = dc.collect_data(2015, 2023, write_csv=False)
    # sample output
    # teams_stats = dc.compute_team_statistics(data)
    dc.collect_and_update_data(2003, 2023)

