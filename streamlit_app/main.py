import sys
import os
import sqlite3
import pandas as pd
import streamlit as st


root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from charts.data_viz import plot_high_level_stats, plot_goals
from data_collection.data_collector import DataCollector
from processing.processing_utils import MatchHistory

st.title('Football Data Analysis')

league = st.radio('Select the league', ('epl', 'serie_a'))

# Initialize the data collector for the selected league
collector = DataCollector(league)
history = MatchHistory(league = league, df = pd.DataFrame())

# Slider for selecting the year range
min_year, max_year = 2003, 2023
year_start, year_end = st.slider('Select the year range', min_value=min_year, max_value=max_year, value=(min_year, max_year))

# Function to get teams
@st.cache_data()
def get_teams(league, year_start, year_end):
    return history.get_teams(league, year_start, year_end)

teams = get_teams(league, year_start, year_end)
home_team = st.selectbox('Select Home Team', teams)
away_team = st.selectbox('Select Away Team', teams)

# Function to fetch head-to-head data
@st.cache_data()
def fetch_head_to_head_data(home_team, away_team, year_start, year_end, total=True):
    return history.fetch_head_to_head_data(home_team, away_team, year_start=year_start, year_end=year_end, total=True)

# Assuming you've fetched the league data already
with st.spinner('Fetching data...'):
    league_data = history.fetch_league_data(league, year_start, year_end)
    if not league_data.empty:
        # Use MatchHistory class for head-to-head analysis
        match_history = MatchHistory(league, league_data)
        head_to_head_data = match_history.fetch_head_to_head_data(home_team, away_team, year_start, year_end, total=True)
        stats = match_history.match_stats(head_to_head_data, home_team, away_team)
        
        if st.button('Show Head-to-Head Stats'):
            if not stats.empty:
                st.dataframe(stats)
            else:
                st.write("No head-to-head data available for the selected teams.")
        
        if st.button('Show League-Wide Stats'):
            # Display league-wide statistics
            league_stats = collector.compute_team_statistics(league_data)
            st.dataframe(league_stats)
    else:
        st.write("No data available for the selected range.")

