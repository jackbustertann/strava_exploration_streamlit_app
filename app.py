# imports
import streamlit as st

import json
import pandas as pd
from jinja2 import Template
from datetime import datetime, date, timedelta
import plotly.express as px

from google.oauth2 import service_account
from google.cloud import bigquery

# initiating GCP client
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# functions

# run a BQ query
@st.experimental_memo(ttl=0)
def run_query(query):

    # run query
    query_job = client.query(query)

    # return result
    rows_raw = query_job.result()

    # convert result into a list of dicts
    rows = [dict(row) for row in rows_raw]

    return rows

# reading config file
with open('config.json', 'r') as f:
    config = json.load(f)

st.set_page_config(
)

last_updated_query = """
    SELECT MAX(date_day) AS last_updated
    FROM `strava-exploration-v2.strava_prod.fct_daily_metrics` """

last_updated_date_str = run_query(last_updated_query)[0]['last_updated']

last_updated_date = datetime.strptime(last_updated_date_str, '%Y-%m-%d')

last_updated_date_delta = (datetime.now() - last_updated_date).days

st.metric(
    label = 'Last Activity Date',
    value = last_updated_date_str,
    delta = f'{last_updated_date_delta} days ago',
    delta_color = "inverse"
)

st.markdown("# Chart 1: Progress Over Time")

filter_dicts = config['filters']

filter_inputs = {}
for filter_dict in filter_dicts:

    filter_name = filter_dict['name']
    filter_input_type = filter_dict['input_type']
    filter_query = filter_dict.get('query', None)
    filter_default = filter_dict.get('default_value', None)
    filter_datatype = filter_dict.get('datatype', None)

    if filter_query is not None:

        if filter_input_type != 'slider':

            filter_options = [option[filter_name] for option in run_query(filter_query)]
        
        else:

            query_df = pd.DataFrame(run_query(filter_query))

            min_value, max_value = query_df.loc[0, 'min_value'], query_df.loc[0, 'max_value']

            if filter_datatype == 'datetime':

                min_value, max_value = datetime.strptime(min_value, "%Y-%m-%d"), datetime.strptime(max_value, "%Y-%m-%d")

                filter_default = max_value - timedelta(days = 365)

            filter_options = (min_value, max_value)

    else:

        filter_options = filter_dict['options']

    filter_name_str = filter_name.replace('_', ' ').capitalize()

    if filter_input_type == 'multiselect':

        if filter_default is not None:

            filter_input = st.sidebar.multiselect(
                f'Pick a {filter_name_str}',
                filter_options,
                filter_default
                )
        else:

            filter_input = st.sidebar.multiselect(
                f'Pick a {filter_name_str}',
                filter_options,
                filter_options
                )
    
    elif filter_input_type == 'radio':

        filter_default_index = filter_options.index(filter_default)

        filter_input = st.sidebar.radio(
            f'Pick a {filter_name_str}',
            filter_options,
            filter_default_index
            )
    
    elif filter_input_type == 'slider':

        if filter_datatype == 'datetime':

            filter_input = st.sidebar.slider(
                f"Select a {filter_name_str}",
                min_value = filter_options[0],
                max_value = filter_options[1],
                value = (filter_default, filter_options[1]),
                format="YYYY-MM-DD"
                )
    
    filter_inputs[filter_name] = filter_input

query_template = Template(

"""
    SELECT 
        {{ filter_inputs['date_granularity'] }}, 
        {{ filter_inputs['dimension'] }}, 
        SUM({{ filter_inputs['measure'] }}) AS {{ filter_inputs['measure'] }}
    FROM `strava-exploration-v2.strava_prod.fct_daily_metrics`
    WHERE 1 = 1
    {%- for key, value in filter_inputs.items() -%}
        {%- if key not in ('date_granularity', 'measure', 'dimension', 'date_range') -%}
            {%- if None in value -%}
                {%- set value_str = value | select('string') | join('", "') -%}
        AND ( {{ key }} in ("{{ value_str }}") OR {{ key }} IS NULL )
            {%- else -%}
                {%- set value_str = value | join('", "') -%}
        AND {{ key }} in ("{{ value_str }}")
            {%- endif -%}
        {%- elif key == 'date_range' -%}
            {%- set min_date = value[0].strftime('%Y-%m-%d') -%}
            {%- set max_date = value[1].strftime('%Y-%m-%d') -%}
        AND CAST(date_day AS DATE) BETWEEN CAST("{{ min_date }}" AS DATE) AND CAST("{{ max_date }}" AS DATE)
        {%- endif %}
    {% endfor %}
    GROUP BY {{ filter_inputs['date_granularity'] }}, {{ filter_inputs['dimension'] }}
    ORDER BY {{ filter_inputs['date_granularity'] }}

"""
)

query_str = query_template.render(
    filter_inputs = filter_inputs)

query_df = pd.DataFrame(run_query(query_str))

n_rows = len(query_df)

if n_rows > 0:

    query_df.columns = [col.replace('_', ' ') for col in list(query_df.columns)]

    query_columns = list(query_df.columns)

    fig = px.bar(
        query_df, 
        x = query_columns[0], 
        y = query_columns[2], 
        color = query_columns[1])

    st.plotly_chart(fig)

else: 

    st.text('No data to display!')

see_query = st.checkbox('See Query', True)

if see_query:

    st.code(query_str, language = 'sql')
