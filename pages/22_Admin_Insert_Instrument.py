import streamlit as st
from utils.db.instrument_db_util import insert_instrument

st.title("Insert New Instrument")

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
