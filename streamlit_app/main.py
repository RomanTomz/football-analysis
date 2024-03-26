import sys
import os
import sqlite3
import pandas as pd
import streamlit as st

root_path = os.path.dirname(os.getcwd())
sys.path.append(root_path)

from data_collection.data_collector import DataCollector
from processing.processing_utils import MatchHistory

st.title('Football Data Analysis')

# Radio button for league selection
league = st.radio('Select the league', ('epl', 'serie_a'))

# Initialize the data collector for the selected league
collector = DataCollector(league)

# Slider for selecting the year range
min_year, max_year = 2003, 2023
year_start, year_end = st.slider('Select the year range', min_value=min_year, max_value=max_year, value=(min_year, max_year))

@st.cache_data()
def fetch_data(league, year_start, year_end):
    # Connect to the SQLite database
    db_path = os.path.join(root_path, 'data_collection', 'data', f'{league}.db')
    conn = sqlite3.connect(db_path)
    
    # Fetch data for the selected year range and league
    table_name = f"{league}_data"
    query = f"SELECT * FROM {table_name} WHERE season >= '{year_start}' AND season <= '{year_end}'"
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data

with st.spinner('Fetching data...'):
    data = fetch_data(league, year_start, year_end)
    if not data.empty:
        stats = collector.compute_team_statistics(data)
        st.dataframe(stats)
    else:
        st.write("No data available for the selected range.")
