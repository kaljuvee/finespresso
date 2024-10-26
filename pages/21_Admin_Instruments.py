import streamlit as st
import pandas as pd
from utils.db.instrument_db_util import save_instrument, get_all_instruments, delete_instruments

st.set_page_config(layout="wide")  # Set the page to wide mode

st.title("Admin Instruments")

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

# Sorting options
col1, col2, col3 = st.columns([2, 2, 1])
with col1:
    sort_column = st.selectbox("Sort by", sortable_columns)
with col2:
    sort_order = st.radio("Sort order", ("Ascending", "Descending"), horizontal=True)
with col3:
    if st.button("Add New Row"):
        new_row = pd.DataFrame([[None] * len(df.columns)], columns=df.columns)
        df = pd.concat([df, new_row], ignore_index=True)

# Custom sorting function
def sort_dataframe(df, column, ascending):
    return df.sort_values(by=column, ascending=ascending, key=lambda x: x.astype(str).fillna(''))

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
        save_instrument(edited_df[edited_df['Delete'] == False].drop('Delete', axis=1))
        st.success("Instruments saved successfully!")

with col2:
    if st.button("Delete Selected", key="delete_button"):
        to_delete = edited_df[edited_df['Delete'] == True]
        if not to_delete.empty:
            delete_instruments(to_delete['id'].tolist())
            st.success(f"{len(to_delete)} instruments deleted successfully!")
        else:
            st.warning("No instruments selected for deletion.")

# Display the number of instruments
st.write(f"Total number of instruments: {len(edited_df)}")
