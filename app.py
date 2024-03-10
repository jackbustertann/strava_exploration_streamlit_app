# imports
import streamlit as st

import json
import pandas as pd
import numpy as np
from jinja2 import Template
from datetime import datetime, date, timedelta
import plotly.express as px
import plotly.graph_objects as go

from google.oauth2 import service_account
from google.cloud import bigquery

# initiating GCP client
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

st.set_page_config()

# functions
def run_query(query):

    # run query
    query_job = client.query(query)

    # return result
    rows_raw = query_job.result()

    # convert result into a list of dicts
    rows = [dict(row) for row in rows_raw]

    return pd.DataFrame(rows)

def convert_mins_to_HHMM(
    time_in_mins: int
):
    hours = int(np.floor(time_in_mins / 60))
    mins = time_in_mins - (hours * 60)
    return "{:02d}:{:02d}".format(hours, mins)

def format_rank(
    rank: int
):
    rank_str = str(rank)
    if (rank_str[-1] == '1') & (rank != 11):
        return rank_str + 'st'
    elif (rank_str[-1] == '2') & (rank != 12):
        return rank_str + 'nd'
    elif (rank_str[-1] == '3') & (rank != 13):
        return rank_str + 'rd'
    else:
        return rank_str + 'th'

# Initiate app
st.title("Strava Web App :runner:")

# Run query
weekly_ride_metrics_query = """
SELECT * 
FROM `strava-exploration-v2`.`strava_prod`.`weekly_ride_metrics_wide_to_long`
"""

weekly_ride_metrics_df = run_query(weekly_ride_metrics_query)
weekly_ride_metrics_df['date_week'] = weekly_ride_metrics_df['date_week'].map(
    lambda x: datetime.strptime(x, '%Y-%m-%d').date()
)

st.header("Moving Time")

weekly_moving_time_df = weekly_ride_metrics_df.loc[
    weekly_ride_metrics_df["metric_name"] == 'moving_time'
]

week_max = weekly_moving_time_df["date_week"].max()
week_min = weekly_moving_time_df["date_week"].min()
moving_time_max = int(np.ceil(weekly_moving_time_df["metric_value"].max()/60) * 60)
total_weeks = len(weekly_moving_time_df)

st.write("Parameters:")

param_col_1, param_col_2, param_col_3 = st.columns([2,1,1])

with param_col_1:
    current_week = st.slider(
        "Current Week:",
        value=(week_max),
        max_value=week_max,
        min_value=week_min + timedelta(weeks = 12),
        step = timedelta(weeks = 1)
    )

    current_week_moving_time_df = weekly_moving_time_df.loc[
        weekly_moving_time_df["date_week"] == current_week
    ]

    previous_week = current_week - timedelta(weeks = 1)

    previous_week_moving_time_df = weekly_moving_time_df.loc[
        weekly_moving_time_df["date_week"] == previous_week
    ]

    
with param_col_2:
    n_weeks = st.number_input(
            'Number of weeks:', 
            min_value = 13,
            max_value = int(((current_week - week_min).days / 7) + 1),
            value = 13,
            step = 1
        )  
    start_week = current_week - timedelta(weeks = (n_weeks-1))

    weekly_moving_time_filtered_df = weekly_moving_time_df.loc[
        (weekly_moving_time_df["date_week"] >= start_week) &
        (weekly_moving_time_df["date_week"] <= current_week)
    ].reset_index().drop(columns=["index"]).reset_index()

with param_col_3:
    y_target = st.number_input(
            'Target (mins):', 
            min_value = 0,
            max_value = 600,
            value = 300,
            step = 15
        )

    weekly_moving_time_filtered_df['is_above_target'] = (
        weekly_moving_time_filtered_df['metric_value'] >= y_target
    )

    above_target_weeks = weekly_moving_time_filtered_df['is_above_target'].sum()

    y_target_percent = (above_target_weeks / n_weeks) * 100

st.write("Metrics:")

metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)

with metric_col_1:
    current_week_moving_time = current_week_moving_time_df['metric_value'].iloc[0]

    current_week_target_delta = int(np.floor(current_week_moving_time - y_target))

    st.metric(
        "Current Week", 
        f"{convert_mins_to_HHMM(int(current_week_moving_time))}",
        f"{int(current_week_target_delta)} mins"
    )

with metric_col_2:

    current_week_moving_time_6w = current_week_moving_time_df["metric_agg_6w"].iloc[0]

    previous_week_moving_time_6w = previous_week_moving_time_df["metric_agg_6w"].iloc[0]

    current_week_6w_delta = current_week_moving_time_6w - previous_week_moving_time_6w
    current_week_6w_delta_percent = round((current_week_6w_delta / current_week_moving_time_6w) * 100, 1)

    st.metric(
        "6W AVG (Short Term)", 
        f"{convert_mins_to_HHMM(int(current_week_moving_time_6w))}",
        f"{current_week_6w_delta_percent} %"
    )

with metric_col_3:

    current_week_moving_time_13w = current_week_moving_time_df["metric_agg_13w"].iloc[0]

    previous_week_moving_time_13w = previous_week_moving_time_df["metric_agg_13w"].iloc[0]

    current_week_13w_delta = current_week_moving_time_13w - previous_week_moving_time_13w
    current_week_13w_delta_percent = round((current_week_13w_delta / current_week_moving_time_13w) * 100, 1)

    st.metric(
        "13W AVG (Mid Term)", 
        f"{convert_mins_to_HHMM(int(current_week_moving_time_13w))}",
        f"{current_week_13w_delta_percent} %"
    )

with metric_col_4:

    current_week_moving_time_26w = current_week_moving_time_df["metric_agg_26w"].iloc[0]

    previous_week_moving_time_26w = previous_week_moving_time_df["metric_agg_26w"].iloc[0]

    current_week_26w_delta = current_week_moving_time_26w - previous_week_moving_time_26w
    current_week_26w_delta_percent = round((current_week_26w_delta / current_week_moving_time_26w) * 100, 1)

    st.metric(
        "26W AVG (Long Term)",
        f"{convert_mins_to_HHMM(int(current_week_moving_time_26w))}",
        f"{current_week_26w_delta_percent} %"
    )

checkbox_col_1, checkbox_col_2, checkbox_col_3, checkbox_col_4 = st.columns(4)

with checkbox_col_2:
    checkbox_2 = st.checkbox(
      label="Show", key="checkbox_2"  
    )

with checkbox_col_3:
    checkbox_3 = st.checkbox(
      label="Show", key="checkbox_3"  
    )
    
with checkbox_col_4:
    checkbox_4 = st.checkbox(
      label="Show", key="checkbox_4"  
    )

metric_col_21, metric_col_22, metric_col_23, metric_col_24 = st.columns(4)

with metric_col_21:

    st.metric(
        "Overall rank:",
        "N/A"
    )

    st.checkbox(
        "Express as %"
    )

with metric_col_22:

    st.metric(
        "6W rank (Short Term):",
        "N/A"
    )

with metric_col_23:

    st.metric(
        "13W rank (Mid Term):",
        "N/A"
    )

with metric_col_24:

    st.metric(
        "26W rank (Long Term):",
        "N/A"
    )


chart_tab_1, chart_tab_2 = st.tabs(
    ["Weekly View", "Ranked View"]
)
    
with chart_tab_1:
    st.markdown(f"I have exceeded a moving time of **{int(y_target)} mins** for **{above_target_weeks} out of {n_weeks} weeks** (**{int(y_target_percent)}%**)")

    
    fig_1a = px.bar(
        weekly_moving_time_filtered_df.loc[
            weekly_moving_time_filtered_df['is_above_target']
        ], 
        x="date_week", 
        y="metric_value",
        color_discrete_sequence = ["#4a8dff"]
        )
    fig_1b = px.bar(
        weekly_moving_time_filtered_df.loc[
            ~weekly_moving_time_filtered_df['is_above_target']
        ], 
        x="date_week", 
        y="metric_value",
        color_discrete_sequence = ["#cdddff"]
        )
    fig_2 = px.line(
        weekly_moving_time_filtered_df, 
        x="date_week", 
        y="metric_agg_6w",
        color_discrete_sequence = ["#b5e853"]
        )
    fig_3 = px.line(
        weekly_moving_time_filtered_df, 
        x="date_week", 
        y="metric_agg_26w",
        color_discrete_sequence = ["#b5e853"]
        )
    fig_4 = px.line(
        weekly_moving_time_filtered_df, 
        x="date_week", 
        y="metric_agg_26w",
        color_discrete_sequence = ["#b5e853"]
        )
    fig_data = fig_1a.data + fig_1b.data
    if checkbox_2:
        fig_data += fig_2.data
    if checkbox_3:
        fig_data += fig_3.data
    if checkbox_4:
        fig_data += fig_4.data
    
    fig = go.Figure(data=fig_data)
    
    fig.add_hline(y=y_target, line_dash='dash')
    
    fig.update_layout(
        title = "",
        xaxis = dict(
            title="Training Week"
        ),
        yaxis = {
            "title": 'Moving Time (mins)',
            "tickvals": list(range(0, moving_time_max + 60, 60)),
            "range": (-15, moving_time_max)
        },
        margin = {
            "t": 0
        },
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

with chart_tab_2:

    weekly_moving_time_ranked_df = (
        weekly_moving_time_filtered_df
        .sort_values(by = ['metric_value', 'date_week'], ascending = [False, True])
    )


    weekly_moving_time_ranked_df["rank"] = (
        weekly_moving_time_ranked_df['metric_value']
        .rank(method="first", ascending = False)
    )

    max_rank = weekly_moving_time_ranked_df["rank"].max()
    current_week_rank = weekly_moving_time_ranked_df.loc[
        weekly_moving_time_ranked_df["date_week"] == current_week
    ]["rank"].iloc[0]
    current_week_rank_percent_str = (
        "top " + str(int(np.ceil((current_week_rank / n_weeks) * 100))) + "%" if current_week_rank <= n_weeks/2
        else "bottom " + str(int(np.ceil((1 - ((current_week_rank - 1) / n_weeks)) * 100))) + "%"
    )

    st.markdown(f"My current week moving time of **{int(current_week_moving_time)} mins** ranks **{format_rank(int(current_week_rank))} out of {n_weeks} weeks** (**{current_week_rank_percent_str}**)")

    fig_1a = px.bar(
        weekly_moving_time_ranked_df.loc[
            weekly_moving_time_ranked_df["date_week"] == current_week
        ], 
        x="rank", 
        y="metric_value",
        color_discrete_sequence = ["#4a8dff"]
        )
    fig_1b = px.bar(
        weekly_moving_time_ranked_df.loc[
            weekly_moving_time_ranked_df["date_week"] != current_week
        ], 
        x="rank", 
        y="metric_value",
        color_discrete_sequence = ["#cdddff"]
        )

    fig_data = fig_1a.data + fig_1b.data
    
    fig = go.Figure(data=fig_data)
    
    fig.add_hline(y=y_target, line_dash='dash')

    xtick_vals = [
        int(x) for x in np.linspace(start=1, stop=max_rank, num=6)
    ]

    xtick_labels = [
        format_rank(x) for x in xtick_vals
    ]
    
    
    fig.update_layout(
        title = "",
        xaxis = dict(
            title="Rank",
            tickvals=xtick_vals,
            ticktext=xtick_labels
        ),
        yaxis = {
            "title": 'Moving Time (mins)',
            "tickvals": list(range(0, moving_time_max + 60, 60)),
            "range": (-15, moving_time_max)
        },
        margin = {
            "t": 0
        },
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)