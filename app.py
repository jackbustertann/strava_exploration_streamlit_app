# Motivating Q's
# How consistently am I achieving my weekly volume target?
# How is my weekly volume trending in the short, mid and long term? 
# How does my current weekly volume rank against the past x weeks?

# Metrics
# Total moving time
# Longest weekly ride
# Number of rides
# Outdoor distance

# TODO's
# Add parameters [x]
# - current week slider [x]
# - time window number input [x]
# - target value number input [x]
# Add moving averages (1W, 6W, 13W, 26W) [x]
# Add ranks (overall, 6W, 13W, 26W) [x]
# - add percentile option [x]
# Create weekly view [x]
# - add desc with % of weeks above target [x]
# - add bars for weekly values, sorted by week [x]
# - add target line [x]
# - add highlighting for weekly values under/over target [x]
# - add trend lines for moving averages [x]
# Create ranked view [x]
# - add desc with rank + percentile for current week [x]
# - add bars for weekly values, sorted by rank [x]
# - add highlighting for current weekly value [x]
# Generalise view for other metrics [x]
# Add query caching [x]


# imports
import streamlit as st

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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
@st.cache_data
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

def format_metric_value(
    metric_value,
    metric_name: str = "",
    metric_unit: str = ""
):

    if metric_name == 'moving_time':
        return convert_mins_to_HHMM(int(metric_value))
    
    elif metric_name in ['max_outdoor_distance', 'active_days']:
        return f"{round(metric_value, 1)} {metric_unit}"

    return f"{int(metric_value)} {metric_unit}"


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

def express_rank_as_top_or_bottom_percent(
    rank: int,
    n_weeks: int
):

    rank_percent_top = (rank / n_weeks) * 100
    rank_percent_bottom = (1 - ((rank - 1) / n_weeks)) * 100
    rank_percent_with_direction = rank_percent_top if rank <= n_weeks/2 else rank_percent_bottom
    rank_prefix = "top" if rank <= n_weeks/2 else "bottom"

    return f"{rank_prefix} {str(int(np.ceil(rank_percent_with_direction)))}%"

# Initiate app
st.title("Strava Web App 🚴‍♂️")

# Run query
weekly_ride_metrics_query = """
SELECT * 
FROM `strava-exploration-v2`.`strava_prod`.`weekly_ride_metrics_wide_to_long`
"""

weekly_ride_metrics_df = run_query(weekly_ride_metrics_query)

weekly_ride_metrics_df['date_week'] = weekly_ride_metrics_df['date_week'].map(
    lambda x: datetime.strptime(x, '%Y-%m-%d').date()
)

week_max = weekly_ride_metrics_df["date_week"].max()
week_min = weekly_ride_metrics_df["date_week"].min()

st.divider()

param_col_11, param_col_12 = st.columns([3,1])

with param_col_11:
    current_week = st.slider(
        "Current Week",
        value=(week_max),
        max_value=week_max,
        min_value=week_min + timedelta(weeks = 12),
        step = timedelta(weeks = 1)
    )

    
with param_col_12:
    n_weeks = st.number_input(
        'Number of Weeks', 
        min_value = 13,
        max_value = int(((current_week - week_min).days / 7) + 1),
        value = 13,
        step = 1
    )  

page_params = {
    "moving_time": {
        "unit": "mins",
        "target_value": {
            "min_value": 0,
            "max_value": 600,
            "default_value": 300,
            "step": 15
        },
        "fig": {
            "metric_show_name": "Total Moving Time",
            "tick_gap": 60,
            "zero_buffer": 15
        }
    },
    "max_outdoor_distance": {
        "unit": "km",
        "target_value": {
            "min_value": 0,
            "max_value": 160,
            "default_value": 80,
            "step": 5
        },
        "fig": {
            "metric_show_name": "Longest Outdoor Ride",
            "tick_gap": 10,
            "zero_buffer": 5
        }
    },
    "active_days": {
        "unit": "days",
        "target_value": {
            "min_value": 0,
            "max_value": 7,
            "default_value": 3,
            "step": 1
        },
        "fig": {
            "metric_show_name": "Active Days",
            "tick_gap": 1,
            "zero_buffer": 1
        }
    }
}

param_col_21, param_col_22 = st.columns([3, 1])

with param_col_21:

    metric_names = list(page_params.keys())

    metric_name = st.radio(
        'Select a Metric',
        metric_names,
        horizontal=True
    )

with param_col_22:

    target_metric_value = st.number_input(
        label = f'Set a Target ({page_params[metric_name]["unit"]})', 
        min_value = page_params[metric_name]["target_value"]["min_value"],
        max_value = page_params[metric_name]["target_value"]["max_value"],
        value = page_params[metric_name]["target_value"]["default_value"],
        step = page_params[metric_name]["target_value"]["step"]
    )

def generate_volume_metrics_page(
    weekly_ride_metrics_df,
    current_week,
    n_weeks,
    target_metric_value,
    page_params,
    metric_name
):

    total_weeks = weekly_ride_metrics_df['date_week'].nunique()

    start_week = current_week - timedelta(weeks = (n_weeks-1))

    current_week_metric_dict = weekly_ride_metrics_df.loc[
        (weekly_ride_metrics_df["metric_name"] == metric_name) &
        (weekly_ride_metrics_df["date_week"] == current_week)
    ].to_dict('records')[0]

    previous_week = current_week - timedelta(weeks = 1)

    previous_week_metric_dict = weekly_ride_metrics_df.loc[
        (weekly_ride_metrics_df["metric_name"] == metric_name) &
        (weekly_ride_metrics_df["date_week"] == previous_week)
    ].to_dict('records')[0]

    weekly_metric_values_for_date_range_df = weekly_ride_metrics_df.loc[
        (weekly_ride_metrics_df["metric_name"] == metric_name) &
        (weekly_ride_metrics_df["date_week"] >= start_week) &
        (weekly_ride_metrics_df["date_week"] <= current_week)
    ].reset_index().drop(columns=["index"]).reset_index()

    max_metric_value = weekly_ride_metrics_df.loc[
        (weekly_ride_metrics_df["metric_name"] == metric_name)
    ]["metric_value"].max()

    n_weeks_above_target = int(weekly_metric_values_for_date_range_df.loc[
        weekly_metric_values_for_date_range_df['metric_value'] >= target_metric_value
    ]["date_week"].nunique())

    n_weeks_above_target_percent = (n_weeks_above_target / n_weeks) * 100

    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns([1.25, 1, 1, 1])

    with metric_col_1:

        current_week_metric_value = current_week_metric_dict['metric_value']

        current_week_target_delta = int(np.floor(current_week_metric_value - target_metric_value))

        st.metric(
            "Current Week Value", 
            format_metric_value(
                current_week_metric_value, 
                metric_name = metric_name,
                metric_unit = page_params["unit"]
            ),
            format_metric_value(
                current_week_metric_value, 
                metric_unit = page_params["unit"]
            )
        )

    with metric_col_2:

        current_week_metric_value_6w = current_week_metric_dict["metric_agg_6w"]

        previous_week_moving_time_6w = previous_week_metric_dict["metric_agg_6w"]

        current_week_6w_delta = current_week_metric_value_6w - previous_week_moving_time_6w
        current_week_6w_delta_percent = round((current_week_6w_delta / current_week_metric_value_6w) * 100, 1)

        st.metric(
            "6W AVG (Short Term)", 
            format_metric_value(
                current_week_metric_value_6w, 
                metric_name = metric_name,
                metric_unit = page_params["unit"]
            ),
            f"{current_week_6w_delta_percent} %"
        )

    with metric_col_3:

        current_week_metric_value_13w = current_week_metric_dict["metric_agg_13w"]

        previous_week_moving_time_13w = previous_week_metric_dict["metric_agg_13w"]

        current_week_13w_delta = current_week_metric_value_13w - previous_week_moving_time_13w
        current_week_13w_delta_percent = round((current_week_13w_delta / current_week_metric_value_13w) * 100, 1)

        st.metric(
            "13W AVG (Mid Term)", 
            format_metric_value(
                current_week_metric_value_13w, 
                metric_name = metric_name,
                metric_unit = page_params["unit"]
            ),
            f"{current_week_13w_delta_percent} %"
        )

    with metric_col_4:

        current_week_metric_value_26w = current_week_metric_dict["metric_agg_26w"]

        previous_week_moving_time_26w = previous_week_metric_dict["metric_agg_26w"]

        current_week_26w_delta = current_week_metric_value_26w - previous_week_moving_time_26w
        current_week_26w_delta_percent = round((current_week_26w_delta / current_week_metric_value_26w) * 100, 1)

        st.metric(
            "26W AVG (Long Term)",
            format_metric_value(
                current_week_metric_value_26w, 
                metric_name = metric_name,
                metric_unit = page_params["unit"]
            ),
            f"{current_week_26w_delta_percent} %"
        )

    checkbox_col_1, checkbox_col_2, checkbox_col_3, checkbox_col_4 = st.columns([1.25, 1, 1, 1])

    with checkbox_col_1:
        checkbox_1 = st.checkbox(
        label="Express as %", key=f"checkbox_1_{metric_name}"  
        )

    with checkbox_col_2:
        checkbox_2 = st.checkbox(
        label="Show Line", key=f"checkbox_2_{metric_name}"    
        )

    with checkbox_col_3:
        checkbox_3 = st.checkbox(
        label="Show Line", key=f"checkbox_3_{metric_name}"   
        )
        
    with checkbox_col_4:
        checkbox_4 = st.checkbox(
        label="Show Line", key=f"checkbox_4_{metric_name}"    
        )

    metric_col_21, metric_col_22, metric_col_23, metric_col_24 = st.columns([1.25, 1, 1, 1])

    with metric_col_21:

        current_week_rank_overall = current_week_metric_dict['metric_rank_overall']
        current_week_rank_overall_percent = express_rank_as_top_or_bottom_percent(current_week_rank_overall, total_weeks)

        if not checkbox_1:
            st.metric(
                "Overall RANK:",
                f"{format_rank(int(current_week_rank_overall))}",
            )
        
        else:

            st.metric(
                "Overall PERCENTILE:",
                f"{current_week_rank_overall_percent}",
            )

    with metric_col_22:

        current_week_rank_6w = current_week_metric_dict['metric_rank_6w']

        st.metric(
            "6W RANK (Short Term):",
            f"{format_rank(int(current_week_rank_6w))}",
        )

    with metric_col_23:

        current_week_rank_13w = current_week_metric_dict['metric_rank_13w']

        st.metric(
            "13W RANK (Mid Term):",
            f"{format_rank(int(current_week_rank_13w))}",
        )

    with metric_col_24:

        current_week_rank_26w = current_week_metric_dict['metric_rank_26w']

        st.metric(
            "26W RANK (Long Term):",
            f"{format_rank(int(current_week_rank_26w))}",
        )

    st.divider()

    chart_tab_1, chart_tab_2 = st.tabs(
        ["Weekly View", "Ranked View"]
    )

    metric_show_name = page_params["fig"]["metric_show_name"]
        
    with chart_tab_1:
        st.markdown(
            f"I have exceeded **{format_metric_value(target_metric_value, metric_unit=page_params['unit'])}** "
            f"for **{n_weeks_above_target} out of {n_weeks} weeks** (**{round(n_weeks_above_target_percent, 1)}%**)"
        )

        
        fig_1a = px.bar(
            weekly_metric_values_for_date_range_df.loc[
                weekly_metric_values_for_date_range_df['metric_value'] >= target_metric_value
            ], 
            x="date_week", 
            y="metric_value",
            color_discrete_sequence = ["#4a8dff"]
            )
        fig_1b = px.bar(
            weekly_metric_values_for_date_range_df.loc[
                weekly_metric_values_for_date_range_df['metric_value'] < target_metric_value
            ], 
            x="date_week", 
            y="metric_value",
            color_discrete_sequence = ["#cdddff"]
            )
        fig_2 = px.line(
            weekly_metric_values_for_date_range_df, 
            x="date_week", 
            y="metric_agg_6w",
            color_discrete_sequence = ["red"]
            )
        fig_3 = px.line(
            weekly_metric_values_for_date_range_df, 
            x="date_week", 
            y="metric_agg_13w",
            color_discrete_sequence = ["red"]
            )
        fig_4 = px.line(
            weekly_metric_values_for_date_range_df, 
            x="date_week", 
            y="metric_agg_26w",
            color_discrete_sequence = ["red"]
            )
        fig_data = fig_1a.data + fig_1b.data
        if checkbox_2:
            fig_data += fig_2.data
        if checkbox_3:
            fig_data += fig_3.data
        if checkbox_4:
            fig_data += fig_4.data
        
        fig = go.Figure(data=fig_data)
        
        fig.add_hline(y=target_metric_value, line_dash='dash')

        fig_params = page_params["fig"]
        tick_gap = fig_params["tick_gap"]
        zero_buffer = fig_params["zero_buffer"]
        
        fig.update_layout(
            title = "",
            xaxis = dict(
                title="Training Week"
            ),
            yaxis = {
                "title": f'{metric_show_name} ({page_params["unit"]})',
                "tickvals": list(range(0, int(np.ceil(max_metric_value/tick_gap) * tick_gap) + tick_gap, tick_gap)),
                "range": (-zero_buffer, int(np.ceil(max_metric_value/tick_gap) * tick_gap))
            },
            margin = {
                "t": 0
            },
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)

    with chart_tab_2:

        weekly_metric_value_ranked_df = (
            weekly_metric_values_for_date_range_df
            .sort_values(by = ['metric_value', 'date_week'], ascending = [False, True])
        )


        weekly_metric_value_ranked_df["rank"] = (
            weekly_metric_value_ranked_df['metric_value']
            .rank(method="first", ascending = False)
        )

        current_week_rank = int(
            weekly_metric_value_ranked_df.loc[
                weekly_metric_value_ranked_df["date_week"] == current_week
            ].to_dict('records')[0]["rank"]
        )

        current_week_rank_percent = express_rank_as_top_or_bottom_percent(current_week_rank, n_weeks)

        st.markdown(
            f"My current week value of **{format_metric_value(current_week_metric_value, metric_unit=page_params['unit'])}** "
            f"ranks **{format_rank(current_week_rank)} out of {n_weeks} weeks** (**{current_week_rank_percent}**)"
        )

        fig_1a = px.bar(
            weekly_metric_value_ranked_df.loc[
                weekly_metric_value_ranked_df["date_week"] == current_week
            ], 
            x="rank", 
            y="metric_value",
            color_discrete_sequence = ["#4a8dff"]
            )
        fig_1b = px.bar(
            weekly_metric_value_ranked_df.loc[
                weekly_metric_value_ranked_df["date_week"] != current_week
            ], 
            x="rank", 
            y="metric_value",
            color_discrete_sequence = ["#cdddff"]
            )

        fig_data = fig_1a.data + fig_1b.data
        
        fig = go.Figure(data=fig_data)
        
        fig.add_hline(y=target_metric_value, line_dash='dash')

        xtick_vals = [
            int(x) for x in np.linspace(start=1, stop=n_weeks, num=6)
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
                "title": f'{metric_show_name} ({page_params["unit"]})',
                "tickvals": list(range(0, int(np.ceil(max_metric_value/tick_gap) * tick_gap) + tick_gap, tick_gap)),
                "range": (-zero_buffer, int(np.ceil(max_metric_value/tick_gap) * tick_gap))
            },
            margin = {
                "t": 0
            },
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)

st.divider()

generate_volume_metrics_page(
    weekly_ride_metrics_df,
    current_week,
    n_weeks,
    target_metric_value,
    page_params[metric_name],
    metric_name
)