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
@st.experimental_memo(ttl=600)
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

is_percent = st.checkbox('Convert to Percent', False)

st.markdown("## Chart 1: Training Volume and Load")

filter_dicts = config['filters']

filter_inputs = {}
for filter_dict in filter_dicts:

    filter_name = filter_dict['name']
    filter_input_type = filter_dict['input_type']
    filter_query = filter_dict.get('query', None)
    filter_default = filter_dict.get('default_value', None)
    filter_datatype = filter_dict.get('datatype', None)

    if filter_query is not None:

        if filter_input_type in ['multiselect', 'radio', 'selectbox']:

            filter_options = [option[filter_name] for option in run_query(filter_query)]
        
        elif filter_input_type == 'slider':

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
    elif filter_input_type == 'selectbox':

        filter_default_index = filter_options.index(filter_default)

        filter_input = st.sidebar.selectbox(
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

measure_filter_dict = [filter_dict for filter_dict in filter_dicts if filter_dict['name'] == 'measure'][0]
measure_options, measure_aliases = measure_filter_dict['options'], measure_filter_dict['aliases']

measure_input = filter_inputs['measure']
measure_input_index = measure_options.index(measure_input)
measure_alias = measure_aliases[measure_input_index]

dimension_input = filter_inputs['dimension']

dimension_colors = {
    "sport": px.colors.qualitative.Plotly, 
    "distance_type": px.colors.sequential.Purples[-3:],
    "workout_type": px.colors.sequential.Oranges[-7:]
    }

dimension_color = dimension_colors[dimension_input]

query_template_1 = Template(

"""
{% if is_percent %}
SELECT 
{{ filter_inputs['date_granularity'] }}, 
{{ filter_inputs['dimension'] }},
{{ filter_inputs['measure'] }} / SUM({{ filter_inputs['measure'] }}) OVER(PARTITION BY {{ filter_inputs['date_granularity'] }}) AS {{ filter_inputs['measure'] }}
FROM (
{% endif %}
    SELECT 
        {{ filter_inputs['date_granularity'] }}, 
        {{ filter_inputs['dimension'] }}, 
        SUM({{ filter_inputs['measure'] }}) AS {{ filter_inputs['measure'] }}
    FROM `strava-exploration-v2.strava_prod.fct_daily_metrics`
    WHERE 1 = 1
    {%- for key, value in filter_inputs.items() -%}
        {%- if key not in ('date_granularity', 'measure', 'dimension', 'date_range', 'zone_type') -%}
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
    ORDER BY {{ filter_inputs['date_granularity'] }}, {{ filter_inputs['dimension'] }}
{% if is_percent %}
) AS agg_date_values
ORDER BY {{ filter_inputs['date_granularity'] }}, {{ filter_inputs['dimension'] }}
{% endif %}
"""
)

query_str_1 = query_template_1.render(
    filter_inputs = filter_inputs,
    is_percent = is_percent)

query_df_1 = pd.DataFrame(run_query(query_str_1))

n_rows_1 = len(query_df_1)

if n_rows_1 > 0:

    query_df_1.columns = [col.replace('_', ' ') for col in list(query_df_1.columns)]

    query_columns_1 = list(query_df_1.columns)

    fig_1 = px.bar(
        query_df_1, 
        x = query_columns_1[0], 
        y = query_columns_1[2], 
        color = query_columns_1[1],
        color_discrete_sequence = dimension_color)

    if is_percent:

        fig_1.update_layout(yaxis_tickformat = 'p')
    
    else:

        fig_1.update_layout(yaxis_ticksuffix = measure_alias)

    
    st.plotly_chart(fig_1) 

else: 

    st.text('No data to display!')


zone_type_input = filter_inputs['zone_type']

zone_type_colors = {
    "heartrate": px.colors.sequential.Reds[-5:], 
    "pace": px.colors.sequential.Blues[-6:]
    }

zone_type_color = zone_type_colors[zone_type_input]

query_template_2 = Template(

"""
{% if is_percent %}
SELECT 
{{ filter_inputs['date_granularity'] }}, 
zone_index,
time_in_zone / SUM(time_in_zone) OVER(PARTITION BY {{ filter_inputs['date_granularity'] }}) AS time_in_zone
FROM (
{% endif %}
    SELECT 
        {{ filter_inputs['date_granularity'] }}, 
        CAST(zone_index AS STRING) AS zone_index,
        SUM(time_in_zone) AS time_in_zone
    FROM `strava-exploration-v2.strava_prod.fct_daily_zones`
    WHERE 1 = 1
    {%- for key, value in filter_inputs.items() -%}
        {%- if key not in ('date_granularity', 'measure', 'dimension', 'date_range') -%}
            {%- if value is string -%}
        AND {{ key }} = "{{ value }}"
                {%- elif None in value -%}
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
    GROUP BY {{ filter_inputs['date_granularity'] }}, zone_index
    ORDER BY {{ filter_inputs['date_granularity'] }}, zone_index
{% if is_percent %}
) AS agg_date_values
ORDER BY {{ filter_inputs['date_granularity'] }}, zone_index
{% endif %}
"""
)

query_str_2 = query_template_2.render(
    filter_inputs = filter_inputs,
    is_percent = is_percent)

st.markdown(f"## Chart 2: Training Intensity ({zone_type_input.capitalize()})")

query_df_2 = pd.DataFrame(run_query(query_str_2))

n_rows_2  = len(query_df_2)

if n_rows_2 > 0:

    query_df_2.columns = [col.replace('_', ' ') for col in list(query_df_2.columns)]

    query_columns_2 = list(query_df_2.columns)

    fig_2 = px.bar(
        query_df_2, 
        x = query_columns_2[0], 
        y = query_columns_2[2], 
        color = query_columns_2[1],
        color_discrete_sequence = zone_type_color)

    if is_percent:

        fig_2.update_layout(yaxis_tickformat = 'p')
    
    else:

        fig_2.update_layout(yaxis_ticksuffix = "'")

    
    st.plotly_chart(fig_2) 

else: 

    st.text('No data to display!')


