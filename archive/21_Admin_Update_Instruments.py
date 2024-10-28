import streamlit as st
import pandas as pd
from utils.db.instrument_db_util import save_instrument, get_all_instruments, delete_instruments

st.set_page_config(layout="wide")  # Set the page to wide mode

st.title("Update Instruments")

# Add a Refresh button at the top of the page
if st.button("Refresh Data", key="refresh_button"):
    st.rerun()

# Load existing instruments
df = get_all_instruments()

# Remove unnecessary columns and reorder
if '_sa_instance_state' in df.columns:
    df = df.drop('_sa_instance_state', axis=1)

# Reorder columns as requested, now including 'sector'
columns_order = ['ticker', 'yf_ticker', 'issuer', 'id', 'isin', 'asset_class', 'sector', 'exchange', 'exchange_code', 'country', 'url']
df = df[columns_order]

# Add a checkbox column for deletion
df['Delete'] = False

# Define sortable columns
sortable_columns = ['ticker', 'yf_ticker', 'issuer', 'id', 'isin', 'asset_class', 'sector', 'exchange', 'exchange_code', 'country', 'url']

# Custom function to handle None values when sorting
def sort_key(value):
    return '' if value is None else str(value)

# Add multiselect filters
col1, col2, col3 = st.columns(3)
with col1:
    selected_exchanges = st.multiselect("Filter by Exchange", options=sorted(df['exchange'].unique(), key=sort_key))
with col2:
    selected_countries = st.multiselect("Filter by Country", options=sorted(df['country'].unique(), key=sort_key))
with col3:
    selected_sectors = st.multiselect("Filter by Sector", options=sorted(df['sector'].unique(), key=sort_key))

# Apply filters
if selected_exchanges:
    df = df[df['exchange'].isin(selected_exchanges)]
if selected_countries:
    df = df[df['country'].isin(selected_countries)]
if selected_sectors:
    df = df[df['sector'].isin(selected_sectors)]

# Sorting options
col1, col2 = st.columns(2)
with col1:
    sort_column = st.selectbox("Sort by", sortable_columns)
with col2:
    sort_order = st.radio("Sort order", ("Ascending", "Descending"), horizontal=True)

# Custom sorting function
def sort_dataframe(df, column, ascending):
    if column == 'id':
        return df.sort_values(by=column, ascending=ascending, key=lambda x: pd.to_numeric(x, errors='coerce'))
    else:
        return df.sort_values(by=column, ascending=ascending, key=lambda x: x.fillna('').astype(str))

# Sort the dataframe
df_sorted = sort_dataframe(df, sort_column, sort_order == "Ascending")

# Calculate the height of the data editor
num_rows = len(df_sorted)
row_height = 35  # Approximate height of each row in pixels
editor_height = min(num_rows * row_height + 100, 600)  # Set a maximum height

# Display editable dataframe with checkboxes
edited_df = st.data_editor(
    df_sorted,
    hide_index=True,
    num_rows="dynamic",
    use_container_width=True,
    height=editor_height,
    column_config={
        "Delete": st.column_config.CheckboxColumn(
            "Delete",
            help="Select to delete",
            default=False
        ),
        "ticker": st.column_config.TextColumn(
            "Ticker",
            help="Stock ticker symbol",
        ),
        "yf_ticker": st.column_config.TextColumn(
            "YF Ticker",
            help="Yahoo Finance ticker symbol",
        ),
        "issuer": st.column_config.TextColumn(
            "Issuer",
            help="Name of the issuing company",
        ),
        "id": st.column_config.NumberColumn(
            "ID",
            help="Instrument ID",
            disabled=True,
        ),
        "isin": st.column_config.TextColumn(
            "ISIN",
            help="International Securities Identification Number",
        ),
        "asset_class": st.column_config.TextColumn(
            "Asset Class",
            help="Type of asset",
        ),
        "sector": st.column_config.TextColumn(
            "Sector",
            help="Industry sector of the instrument",
        ),
        "exchange": st.column_config.TextColumn(
            "Exchange",
            help="Stock exchange name",
        ),
        "exchange_code": st.column_config.TextColumn(
            "Exchange Code",
            help="Stock exchange code",
        ),
        "country": st.column_config.TextColumn(
            "Country",
            help="Country of the instrument",
        ),
        "url": st.column_config.TextColumn(
            "URL",
            help="Related URL",
        ),
    },
)

# Save and Delete buttons
col1, col2 = st.columns(2)
with col1:
    if st.button("Save Changes", key="save_button"):
        try:
            df_to_save = edited_df[edited_df['Delete'] == False].drop('Delete', axis=1)
            
            updated_instruments = []
            with st.spinner('Updating instruments...'):
                for _, row in df_to_save.iterrows():
                    instrument_data = row.to_dict()
                    updated_instrument = save_instrument(instrument_data)
                    if updated_instrument:
                        updated_instruments.append(updated_instrument)
            
            updated_count = len(updated_instruments)
            st.success(f"Instruments updated successfully! {updated_count} instruments updated.")
            
            if updated_count > 0:
                st.write("Updated Instruments:")
                updated_df = pd.DataFrame(updated_instruments)
                st.dataframe(updated_df)
            else:
                st.warning("No instruments were updated. No changes were necessary.")
            
            # Refresh the data
            st.rerun()
        except Exception as e:
            st.error(f"An error occurred while updating instruments: {str(e)}")
            logger.exception("Error in save_instrument")

with col2:
    if st.button("Delete Selected", key="delete_button"):
        to_delete = edited_df[edited_df['Delete'] == True]
        if not to_delete.empty:
            delete_instruments(to_delete['id'].tolist())
            st.success(f"{len(to_delete)} instruments deleted successfully!")
            st.rerun()
        else:
            st.warning("No instruments selected for deletion.")

# Display the number of instruments
st.write(f"Total number of instruments: {len(edited_df)}")
