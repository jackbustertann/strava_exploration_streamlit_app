# imports
import streamlit as st

import json
import pandas as pd
from jinja2 import Template

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

st.markdown("# Query 1: Progress Over Time")

filter_dicts = config['filters']

filter_inputs = {}
for filter_dict in filter_dicts:

    filter_name = filter_dict['name']
    filter_input_type = filter_dict['input_type']
    filter_query = filter_dict.get('query', None)
    filter_default = filter_dict.get('default_value', None)

    if filter_query is not None:

        filter_options = [option[filter_name] for option in run_query(filter_query)]
    
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
    
    filter_inputs[filter_name] = filter_input

query_template = Template(

"""

SELECT {{ filter_inputs['date_granularity'] }}, SUM({{ filter_inputs['measure'] }})
FROM `strava-exploration-v2.strava_prod.fct_daily_metrics`
WHERE 1 = 1
{% for key, value in filter_inputs.items() -%}
    {% if key not in ('date_granularity', 'measure') %}
        {%- if None in value -%}
            {%- set value_str = value | select('string') | join(', ') -%}
    AND ( {{ key }} in ({{ value_str }}) OR {{ key }} IS NULL )
        {%- else -%}
            {%- set value_str = value | join(', ') -%}
    AND {{ key }} in ({{ value_str }})
        {%- endif -%}
    {% endif %}
{% endfor %}
GROUP BY {{ filter_inputs['date_granularity'] }}

"""
)

query_str = query_template.render(filter_inputs = filter_inputs)

st.code(query_str, language = 'sql')
