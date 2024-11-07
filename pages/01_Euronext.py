import streamlit as st
from utils.display.display_publisher import display_publisher
from datetime import datetime, timedelta

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Streamlit app title
st.title("Euronext Market News")

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Time filter
time_filter = st.radio(
    "Time Range",
    ["Today", "Last Week", "All Time"],
    horizontal=True
)

# Calculate date range based on filter
today = datetime.now().date()
if time_filter == "Today":
    start_date = today
    end_date = today + timedelta(days=1)
elif time_filter == "Last Week":
    start_date = today - timedelta(days=7)
    end_date = today + timedelta(days=1)
else:
    start_date = None
    end_date = None

# Search/filter by ticker
ticker_filter = st.text_input("Filter by Ticker", "")

# Sorting options
sortable_columns = ['Ticker', 'Title', 'Company', 'Expected Move (%)', 
                   'Probability % (Direction)', 'Event', 'Reason', 'Published Date']
col1, col2 = st.columns(2)
with col1:
    sort_column = st.selectbox("Sort by", sortable_columns, 
                              index=sortable_columns.index('Expected Move (%)'))
with col2:
    sort_order = st.radio("Sort order", ("Ascending", "Descending"), 
                         index=1,  # Default to Descending
                         horizontal=True)

# Pagination
items_per_page = 25
page = st.selectbox("Select Page", options=range(1, 101))

# Cached function to display news and get total pages
@st.cache_data(ttl=3600)  # Cache for 1 hour
def cached_display_publisher(publisher, page, items_per_page, start_date, end_date, 
                           ticker_filter, sort_column, sort_ascending):
    try:
        total_pages, df = display_publisher(publisher, page, items_per_page, start_date, end_date,
                                          ticker_filter, sort_column, sort_ascending)
        
        if df is None or df.empty:
            st.warning("No data available for the selected filters.")
            return 0
        
        return total_pages
    except Exception as e:
        st.error(f"An error occurred while fetching the data: {str(e)}")
        return 0

# Display the news and get total pages
total_pages = cached_display_publisher(
    'euronext', 
    page, 
    items_per_page,
    start_date,
    end_date,
    ticker_filter,
    sort_column,
    sort_order == "Ascending"
)

# Display pagination information only if there are pages
if total_pages > 0:
    st.write(f"Page {page} of {total_pages}")
else:
    st.write("No data available")
