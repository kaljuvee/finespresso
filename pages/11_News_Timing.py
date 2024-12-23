import streamlit as st
import plotly.express as px
from utils.db.news_db_util import get_news_df
import pandas as pd
from datetime import datetime, timedelta, time

st.title("News Publication Timing Analysis")

# Time filter
time_filter = st.radio(
    "Time Range",
    ["All Time", "Last Month", "Last Week", "Today"],
    horizontal=True
)

# Get data and prepare filters
df = get_news_df()

# Publisher filter
publishers = ['All Publishers'] + sorted(df['publisher'].unique().tolist())
selected_publisher = st.selectbox("Select Publisher", publishers)

# Event filter - handle null events
df['event'] = df['event'].fillna('Unclassified')
events = ['All Events'] + sorted(df['event'].unique().tolist())
selected_event = st.selectbox("Filter by Event Type", events)

# Calculate date range based on filter
today = datetime.now().date()
if time_filter == "Today":
    start_date = today
elif time_filter == "Last Week":
    start_date = today - timedelta(days=7)
elif time_filter == "Last Month":
    start_date = today - timedelta(days=30)
else:
    start_date = None

# Apply filters
filtered_df = df.copy()

# Time filter
if start_date:
    filtered_df = filtered_df[filtered_df['published_date'].dt.date >= start_date]

# Publisher filter
if selected_publisher != 'All Publishers':
    filtered_df = filtered_df[filtered_df['publisher'] == selected_publisher]

# Event filter
if selected_event != "All Events":
    filtered_df = filtered_df[filtered_df['event'] == selected_event]

# Extract hour from published_date
filtered_df['hour'] = filtered_df['published_date'].dt.hour

# Add market timing classification
def classify_market(hour):
    if 9 <= hour < 16:
        return 'Regular Market'
    elif 16 <= hour <= 23:
        return 'After Market'
    else:  # 0-9
        return 'Pre Market'

# Add market classification to filtered_df
filtered_df['market_timing'] = filtered_df['hour'].apply(classify_market)

# Add market timing filter
market_timing_filter = st.radio(
    "Market Timing",
    ["All", "Regular Market", "After Market", "Pre Market"],
    horizontal=True
)

if market_timing_filter != "All":
    filtered_df = filtered_df[filtered_df['market_timing'] == market_timing_filter]

# Calculate hourly distribution once (needed for summary statistics)
hourly_dist = filtered_df['hour'].value_counts().sort_index().reset_index()
hourly_dist.columns = ['Hour', 'Count']

# View type selection and plotting
view_type = st.radio(
    "View Type",
    ["Hourly Distribution", "Market Timing Distribution"],
    horizontal=True
)

if view_type == "Hourly Distribution":
    fig = px.bar(
        hourly_dist,
        x='Hour',
        y='Count',
        title=f'News Distribution by Hour of Day\n{time_filter}, {selected_publisher}, {selected_event}'
    )
    
    fig.update_layout(
        xaxis_title="Hour of Day (24h format)",
        yaxis_title="Number of News Items",
        bargap=0.2,
        title_x=0.5,
        title_y=0.95
    )
else:
    market_dist = filtered_df['market_timing'].value_counts().reset_index()
    market_dist.columns = ['Market Timing', 'Count']
    
    fig = px.bar(
        market_dist,
        x='Market Timing',
        y='Count',
        title=f'News Distribution by Market Timing\n{time_filter}, {selected_publisher}, {selected_event}'
    )
    
    fig.update_layout(
        xaxis_title="Market Timing",
        yaxis_title="Number of News Items",
        bargap=0.2,
        title_x=0.5,
        title_y=0.95
    )

# Display the plot
st.plotly_chart(fig, use_container_width=True)

# Display summary statistics
st.subheader("Summary Statistics")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total News Items", len(filtered_df))
    if len(hourly_dist) > 0:  # Check if we have any data
        st.metric("Peak Hour", hourly_dist.loc[hourly_dist['Count'].idxmax(), 'Hour'])
    else:
        st.metric("Peak Hour", "N/A")
with col2:
    if len(hourly_dist) > 0:  # Check if we have any data
        st.metric("Average News per Hour", round(hourly_dist['Count'].mean(), 1))
    else:
        st.metric("Average News per Hour", "N/A")
    st.metric("Number of Days", len(filtered_df['published_date'].dt.date.unique()))
with col3:
    # Market timing statistics
    market_stats = filtered_df['market_timing'].value_counts()
    total_news = len(filtered_df)
    
    if total_news > 0:  # Check if we have any data
        st.metric("Regular Market News", 
                f"{market_stats.get('Regular Market', 0)} ({round(market_stats.get('Regular Market', 0)/total_news*100, 1)}%)")
        st.metric("After/Pre Market News", 
                f"{total_news - market_stats.get('Regular Market', 0)} ({round((total_news - market_stats.get('Regular Market', 0))/total_news*100, 1)}%)")
    else:
        st.metric("Regular Market News", "N/A")
        st.metric("After/Pre Market News", "N/A")
