import streamlit as st
from utils.db.instrument_db_util import insert_instrument, get_instrument_by_company_name

st.title("Insert New Instrument")

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
    asset_class = st.text_input("Asset Class")
    sector = st.text_input("Sector")
    exchange = st.text_input("Exchange")
    exchange_code = st.text_input("Exchange Code")
    country = st.text_input("Country")
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
