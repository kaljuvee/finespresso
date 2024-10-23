import streamlit as st
from utils.display.display_util import make_clickable
from utils.db.news_db_util import get_news_df
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

def format_event(event):
    if pd.isna(event):
        return ""
    return event.replace('_', ' ').capitalize()

def display_news_history(df, page, items_per_page):
    # Select the columns we want to display in the desired order
    df_display = df[['Ticker', 'Ticker URL', 'Title', 'Link', 'Predicted Move', 'Reason', 'Published Date', 'Company', 'Event', 'Publisher']]
    
    # Calculate start and end indices for the current page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    
    # Slice the dataframe for the current page
    df_page = df_display.iloc[start_idx:end_idx]
    
    # Make Ticker column clickable
    df_page['Ticker'] = df_page.apply(lambda row: make_clickable(row['Ticker'], row['Ticker URL']), axis=1)
    
    # Make Title column clickable
    df_page['Title'] = df_page.apply(lambda row: make_clickable(row['Title'], row['Link']), axis=1)
    
    # Drop the Ticker URL and Link columns as they're no longer needed
    df_page = df_page.drop(columns=['Ticker URL', 'Link'])
    
    # Format the Predicted Move column
    df_page['Predicted Move'] = df_page['Predicted Move'].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else "")
    
    # Format the Event column
    df_page['Event'] = df_page['Event'].apply(format_event)
    
    # Configure AgGrid
    gb = GridOptionsBuilder.from_dataframe(df_page)
    gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
    gb.configure_column("Ticker", headerName="Ticker", cellRenderer="html")
    gb.configure_column("Title", headerName="Title", cellRenderer="html")
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=items_per_page)
    gridOptions = gb.build()
    
    # Display the AgGrid
    AgGrid(df_page, 
           gridOptions=gridOptions, 
           enable_enterprise_modules=True, 
           update_mode=GridUpdateMode.SELECTION_CHANGED, 
           allow_unsafe_jscode=True,
           theme='streamlit')

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Streamlit app title with clickable link
st.title("Finespresso - Why is it Moving?")
st.subheader("News History")

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.rerun()

# Get the dataframe
df = get_news_df()

# Sort the dataframe by published_date in descending order (most recent first)
df = df.sort_values('published_date', ascending=False)

# Pagination
items_per_page = 25

# Initialize the page number in session state if it doesn't exist
if 'page' not in st.session_state:
    st.session_state.page = 1

# Get unique publishers and create multi-select widget
unique_publishers = sorted(df['publisher'].unique())
selected_publishers = st.multiselect(
    "Select Publishers",
    options=unique_publishers,
    default=unique_publishers,
    key="publisher_select"
)

# Filter the dataframe based on selected publishers
df_filtered = df[df['publisher'].isin(selected_publishers)]

# Calculate total pages based on filtered dataframe
total_pages = len(df_filtered) // items_per_page + (1 if len(df_filtered) % items_per_page > 0 else 0)

# Add a select box for page selection
st.session_state.page = st.selectbox("Select Page", options=range(1, total_pages + 1), index=st.session_state.page - 1, key="page_select")

# Ensure the column names match those expected by display_news_history
df_filtered = df_filtered.rename(columns={
    'title': 'Title',
    'link': 'Link',
    'ticker': 'Ticker',
    'ticker_url': 'Ticker URL',
    'company': 'Company',
    'published_date': 'Published Date',
    'event': 'Event',
    'reason': 'Reason',
    'predicted_move': 'Predicted Move',
    'publisher': 'Publisher'
})

# Display the news
display_news_history(df_filtered, st.session_state.page, items_per_page)
