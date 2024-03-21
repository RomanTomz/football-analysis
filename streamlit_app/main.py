import sys
import os


root_path = os.path.dirname(os.getcwd())

sys.path.append(root_path)


import streamlit as st
from data_collection.data_collector import DataCollector
from processing.processing_utils import MatchHistory

st.title('Football Data Analysis')

min_year, max_year = 2003, 2023
year_start, year_end = st.slider('Select the year range',min_value=min_year, max_value=max_year, value=(min_year, max_year))

collector = DataCollector('epl')

with st.spinner('Fetching data...'):
    data = collector.collect_data(year_start, year_end)
    stats = collector.compute_team_statistics(data)

# Display the statistics
st.dataframe(stats)