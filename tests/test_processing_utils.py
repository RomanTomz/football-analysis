import unittest
from unittest.mock import patch
from processing.processing_utils import MatchHistory
import pandas as pd

class TestMatchHistory(unittest.TestCase):
    def setUp(self):
        self.match_history = MatchHistory(league="serie_a", df=None)

    def test_get_teams_returns_list_of_teams(self):
        teams = self.match_history.get_teams(league="serie_a", year_start=2000, year_end=2022)
        self.assertIsInstance(teams, list)
        self.assertGreater(len(teams), 0)

    def test_fetch_league_data_returns_dataframe(self):
        data = self.match_history.fetch_league_data(league="serie_a", year_start=2000, year_end=2022)
        self.assertIsInstance(data, pd.DataFrame)

    def test_fetch_head_to_head_data_returns_dataframe(self):
        home_team = "Team A"
        away_team = "Team B"
        head_to_head = self.match_history.fetch_head_to_head_data(home_team, away_team)
        self.assertIsInstance(head_to_head, pd.DataFrame)

    def test_match_stats_returns_dataframe(self):
        head_to_head = pd.DataFrame({
            "HomeTeam": ["Team A", "Team B"],
            "AwayTeam": ["Team B", "Team A"],
            "FTR": ["H", "A"]
        })
        home_team = "Team A"
        away_team = "Team B"
        stats = self.match_history.match_stats(head_to_head, home_team, away_team)
        self.assertIsInstance(stats, pd.DataFrame)

if __name__ == "__main__":
    unittest.main()