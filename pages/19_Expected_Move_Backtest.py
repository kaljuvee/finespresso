import streamlit as st
from utils.display_util import display_rate_move
from utils.news_db_util import get_rate_move_df
from utils.filter_util import publisher_whitelist

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Streamlit app title
st.title("Macro Hive Morning Sales Copilot")
st.subheader("Expected Move Backtest")

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.rerun()

# Get the dataframe with rate move data
df = get_rate_move_df()

# Filter the dataframe based on the publisher whitelist
df_filtered = df[df['publisher'].isin(publisher_whitelist)]

# Event filtering
if 'event' in df_filtered.columns:
    unique_events = sorted(df_filtered['event'].dropna().unique())
    default_events = [
        "Trade Agreements and Negotiations",
        "Fiscal Policy Announcements",
        "Trade Balance and International Trade",
        "Intellectual Property and Technology T",
        "Employment Data"
    ]
    # Filter default_events to only include those present in unique_events
    default_events = [event for event in default_events if event in unique_events]
    
    selected_events = st.multiselect(
        "Select Events",
        options=unique_events,
        default=default_events,
        key="rate_move_event_select"
    )
    # Filter the dataframe based on selected events
    df_filtered = df_filtered[df_filtered['event'].isin(selected_events)]
else:
    st.warning("Event information is not available. Showing all data without event filtering.")

# Pagination
items_per_page = 25

# Initialize the page number in session state if it doesn't exist or is not an integer
if 'rate_move_page' not in st.session_state or not isinstance(st.session_state.rate_move_page, int):
    st.session_state.rate_move_page = 1

# Calculate total pages based on filtered dataframe
total_pages = max(1, len(df_filtered) // items_per_page + (1 if len(df_filtered) % items_per_page > 0 else 0))

# Add a select box for page selection, but only if there are pages to select
if total_pages > 0:
    selected_page = st.selectbox(
        "Select Page",
        options=range(1, total_pages + 1),
        index=min(st.session_state.rate_move_page - 1, total_pages - 1),
        key="rate_move_page_select"
    )
    # Update the session state with the selected page
    st.session_state.rate_move_page = int(selected_page)
else:
    st.warning("No data available to display.")
    st.session_state.rate_move_page = 1

# Only display the rate move data if there are pages to show
if total_pages > 0:
    display_rate_move(df_filtered, st.session_state.rate_move_page, items_per_page)
