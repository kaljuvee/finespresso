import streamlit as st
import plotly.express as px
from utils.db.news_db_util import get_news_df
import pandas as pd
from datetime import datetime, timedelta

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

# Extract hour from published_date and create hourly distribution
filtered_df['hour'] = filtered_df['published_date'].dt.hour
hourly_dist = filtered_df['hour'].value_counts().sort_index().reset_index()
hourly_dist.columns = ['Hour', 'Count']

# Create bar chart using plotly
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
    title_x=0.5,  # Center the title
    title_y=0.95  # Adjust title position
)

# Display the plot
st.plotly_chart(fig, use_container_width=True)

# Display summary statistics
st.subheader("Summary Statistics")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total News Items", len(filtered_df))
with col2:
    st.metric("Peak Hour", hourly_dist.loc[hourly_dist['Count'].idxmax(), 'Hour'])
with col3:
    st.metric("Average News per Hour", round(hourly_dist['Count'].mean(), 1))
with col4:
    st.metric("Number of Days", len(filtered_df['published_date'].dt.date.unique()))
