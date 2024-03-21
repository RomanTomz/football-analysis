import sys
import os


root_path = os.path.dirname(os.getcwd())

sys.path.append(root_path)


import streamlit as st
from data_collection.data_collector import DataCollector

collector = DataCollector('epl')
data = collector.collect_data(2003, 2021)
stats = collector.compute_team_statistics(data)
st.title('Football Data Analysis')
st.dataframe(stats)