"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st

st.markdown('# Strava Exploration App')

# Add a selectbox to the sidebar:
add_selectbox = st.sidebar.selectbox(
    'Pick a Sport',
    ('Run', 'Bike', 'Hike')
)

# Add a slider to the sidebar:
add_slider = st.sidebar.slider(
    'distance',
    0.0, 45.0, (8.0, 12.0)
)
