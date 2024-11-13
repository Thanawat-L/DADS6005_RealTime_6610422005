import streamlit as st
from pinotdb import connect
import numpy as np
import pandas as pd
import plotly.express as px
import time

conn = connect(host = '47.129.162.84', port = 8099, path = '/query/sql', schema = 'http')

# DataFrame_01:------------------------------------------------------------
curs_01 = conn.cursor()
query_01 = """SELECT * FROM game_users_tumbling_window_topic LIMIT 100000"""

curs_01.execute(query_01)
result_01 = curs_01.fetchall()
df_01 = pd.DataFrame(result_01, columns =['GAME_NAME','GENRE','PAGE_ID','PLATFORM','PUBLISHER','RATING_COUNT','WINDOWEND','WINDOWSTART','WINDOW_END','WINDOW_START','YEAR','timestamp'])
df_01['timestamp'] = pd.to_datetime(df_01['timestamp'], unit='ms')

pd.set_option('future.no_silent_downcasting', True)

df_01 = df_01.replace("null", np.nan).replace(0, np.nan)
df_01 = df_01.infer_objects(copy=False)
df_01 = df_01.dropna(subset=[col for col in df_01.columns if col not in ['PAGE_ID', 'WINDOWEND', 'WINDOWSTART']])

# Graph_01:------------------------------------------------------------
genre_counts_by_year = df_01.groupby(['YEAR', 'GENRE']).size().unstack().fillna(0)
df_plot = genre_counts_by_year.reset_index().melt(id_vars='YEAR', value_name='Count', var_name='Genre')
df_plot = df_plot[(df_plot['YEAR'] >= 1900) & (df_plot['Genre'].notnull())]

fig_01 = px.bar(
    df_plot,
    x='YEAR',
    y='Count',
    color='Genre',
    title='Number of Games Released per Genre Each Year (From 1900)',
    labels={'Count': 'Number of Games', 'YEAR': 'Year'},
)
fig_01.update_layout(barmode='stack')
fig_01.update_xaxes(rangeslider_visible=True)

# DataFrame_02:------------------------------------------------------------
curs_02 = conn.cursor()
query_02 = """SELECT * FROM game_users_session_window_topic LIMIT 100000"""

curs_02.execute(query_02)
result_02 = curs_02.fetchall()
df_02 = pd.DataFrame(result_02, columns =['GAME_NAME','GENRE','PAGE_ID','PLATFORM','PUBLISHER','RATING_COUNT','SESSION_END_TS','SESSION_LENGTH_MS','SESSION_START_TS','WINDOWEND','WINDOWSTART','YEAR','timestamp'])
df_02['timestamp'] = pd.to_datetime(df_02['timestamp'], unit='ms')

pd.set_option('future.no_silent_downcasting', True)

df_02 = df_02.replace("null", np.nan).replace(0, np.nan)
df_02 = df_02.infer_objects(copy=False)
df_02 = df_02.dropna(subset=[col for col in df_02.columns if col not in ['PAGE_ID', 'WINDOWEND', 'WINDOWSTART']])

# Graph_02:------------------------------------------------------------
genre_counts_by_platform = df_02.groupby(['PLATFORM', 'GENRE']).size().unstack().fillna(0)
df_plot = genre_counts_by_platform.reset_index().melt(id_vars='PLATFORM', value_name='Count', var_name='Genre')

fig_02 = px.bar(
    df_plot,
    x='PLATFORM',
    y='Count',
    color='Genre',
    title='Distribution of Game Genres by Platform',
    labels={'Count': 'Number of Games', 'PLATFORM': 'Platform'},
    color_discrete_sequence=px.colors.qualitative.Pastel
)
fig_02.update_layout(barmode='stack')

# DataFrame_03:------------------------------------------------------------
curs_03 = conn.cursor()
query_03 = """SELECT * FROM game_users_hopping_window_topic LIMIT 100000"""

curs_03.execute(query_03)
result_03 = curs_03.fetchall()
df_03 = pd.DataFrame(result_03, columns =['GAME_NAME','GENRE','PAGE_ID','PLATFORM','PUBLISHER','RATING_COUNT','WINDOWEND','WINDOWSTART','WINDOW_END_TS','WINDOW_LENGTH_MS','WINDOW_START_TS','YEAR','timestamp'])
df_03['timestamp'] = pd.to_datetime(df_03['timestamp'], unit='ms')

pd.set_option('future.no_silent_downcasting', True)

df_03 = df_03.replace("null", np.nan).replace(0, np.nan)
df_03 = df_03.infer_objects(copy=False)
df_03 = df_03.dropna(subset=[col for col in df_03.columns if col not in ['PAGE_ID', 'WINDOWEND', 'WINDOWSTART']])

# DataFrame_04:------------------------------------------------------------
curs_04 = conn.cursor()
query_04 = """SELECT * FROM pageviews_game_users_join_topic LIMIT 100000"""

curs_04.execute(query_04)
result_04 = curs_04.fetchall()
df_04 = pd.DataFrame(result_04, columns =['EU_SALES','GAME_NAME','GENDER','GENRE','GLOBAL_SALES','JP_SALES','NA_SALES','OTHER_SALES','PLATFORM','PUBLISHER','PV_PAGEID','PV_USERID','REGIONID','VIEWTIME','YEAR','timestamp'])
df_04['timestamp'] = pd.to_datetime(df_04['timestamp'], unit='ms')

pd.set_option('future.no_silent_downcasting', True)

df_04 = df_04.replace("null", np.nan).replace(0, np.nan)
df_04 = df_04.infer_objects(copy=False)
df_04 = df_04.dropna(subset=[col for col in df_04.columns if col != 'PV_USERID'])

# Graph_04:------------------------------------------------------------
fig_03 = px.scatter(
    df_04,
    x="NA_SALES",
    y="JP_SALES",
    size="GLOBAL_SALES",
    color="GAME_NAME",
    title="Regional Sales Distribution (NA vs JP)",
    labels={"NA_SALES": "North America Sales (in millions)", "JP_SALES": "Japan Sales (in millions)"},
    hover_name="GAME_NAME",
)

fig_04 = px.pie(
    df_04,
    names="GENRE",
    values="GLOBAL_SALES",
    title="Genre Distribution by Global Sales",
    labels={"GENRE": "Game Genre", "GLOBAL_SALES": "Global Sales (in millions)"},
)

# Streamlit layout
st.set_page_config(page_title="Gundam Views Dashboard", layout="wide")
st.title("Game Sales Analysis")

# Set up auto-refresh options
if "sleep_time" not in st.session_state:
    st.session_state.sleep_time = 30
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True
 
# Create an Auto refresh Button
auto_refresh = st.checkbox('Auto Refresh?', st.session_state.auto_refresh)
st.session_state.auto_refresh = auto_refresh
 
if auto_refresh:
    refresh_rate = st.number_input('Refresh rate in seconds', value=st.session_state.sleep_time, min_value=1)
    st.session_state.sleep_time = refresh_rate
else:
    refresh_rate = st.session_state.sleep_time

# Create Layout
with st.container():
    col1, col2 = st.columns(2) 
    with col1:
        st.plotly_chart(fig_01, use_container_width=True)
    with col2:
        st.plotly_chart(fig_02, use_container_width=True)

with st.container():
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_03, use_container_width=True)
    with col2:
        st.plotly_chart(fig_04, use_container_width=True)

# Refresh logic
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()