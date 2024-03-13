import pandas as pd
import unittest
from unittest.mock import patch
from data_collection.data_collector import DataCollector
import os

class TestDataCollector(unittest.TestCase):
    def setUp(self):
        self.data_collector = DataCollector(league="serie_a")
        self.data_collector.all_data = [
            pd.DataFrame({
                "Date": ["2022-01-01", "2022-01-02"],
                "HomeTeam": ["Team A", "Team B"],
                "FTHG": [2, 1],
                "FTAG": [1, 0]
            })
        ]
        self.data_collector.league = "Premier League"
    def tearDown(self):
        for file in os.listdir():
            if file.endswith(".csv"):
                os.remove(file)

    def test_process_data_returns_dataframe(self):
        result = self.data_collector._process_data(write_csv=False)
        self.assertIsInstance(result, pd.DataFrame)

    def test_process_data_writes_csv_file(self):
        with patch("data_collection.data_collector.uuid") as mock_uuid:
            mock_uuid.uuid4().hex[:8].side_effect = ["abc123", "def456"]
            self.data_collector._process_data(write_csv=True)
            mock_uuid.uuid4.assert_called_with()

    def test_process_data_returns_expected_columns(self):
        expected_columns = [
            "game_id", "Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "TG",
            "city_name", "lat", "lon"
        ]
        result = self.data_collector._process_data(write_csv=False)
        self.assertListEqual(list(result.columns), expected_columns)

if __name__ == "__main__":
    unittest.main()