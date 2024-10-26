import streamlit as st
from utils.db.instrument_db_util import insert_instrument, get_instrument_by_company_name, get_distinct_instrument_fields

st.title("Insert New Instrument")

# Load distinct fields into session state if not already present
if 'distinct_fields' not in st.session_state:
    st.session_state.distinct_fields = get_distinct_instrument_fields()

# Add search functionality
search_company_name = st.text_input("Search by Company Name")
search_button = st.button("Search")

if search_button:
    if search_company_name:
        existing_instrument = get_instrument_by_company_name(search_company_name)
        if existing_instrument:
            st.warning(f"Instrument exists: {existing_instrument.to_dict()}. Use Update Instrument Page to make changes to instruments.")
        else:
            st.success("Instrument not found. You can add a new instrument below.")
    else:
        st.warning("Please enter a company name to search.")

with st.form("insert_instrument_form"):
    issuer = st.text_input("Issuer")
    ticker = st.text_input("Ticker")
    yf_ticker = st.text_input("Yahoo Finance Ticker")
    isin = st.text_input("ISIN")
    asset_class = st.selectbox("Asset Class", [""] + st.session_state.distinct_fields['asset_classes'])
    sector = st.selectbox("Sector", [""] + st.session_state.distinct_fields['sectors'])
    exchange = st.selectbox("Exchange", [""] + st.session_state.distinct_fields['exchanges'])
    exchange_code = st.selectbox("Exchange Code", [""] + st.session_state.distinct_fields['exchange_codes'])
    country = st.selectbox("Country", [""] + st.session_state.distinct_fields['countries'])
    url = st.text_input("URL")

    submit_button = st.form_submit_button("Insert Instrument")

if submit_button:
    instrument_data = {
        "issuer": issuer,
        "ticker": ticker,
        "yf_ticker": yf_ticker,
        "isin": isin,
        "asset_class": asset_class,
        "sector": sector,
        "exchange": exchange,
        "exchange_code": exchange_code,
        "country": country,
        "url": url
    }

    inserted_instrument, message = insert_instrument(instrument_data)

    if inserted_instrument:
        st.success(message)
        st.json(inserted_instrument)
        st.write(f"Company Name: {inserted_instrument['issuer']}")
        st.write(f"Ticker: {inserted_instrument['ticker']}")
        
        if st.button("Add Another Instrument"):
            st.rerun()
    else:
        st.warning(message)
