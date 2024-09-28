import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import random
from utils.db_util import save_email

# Function to generate random data
def generate_random_data(n=100):
    tickers = [f"TICK{i:03d}" for i in range(1, n+1)]
    exchanges = ["Euronext", "Nasdaq Baltics", "Nasdaq Nordic", "LSEG", "SIX", "Deutsche Börse"]
    data = {
        "ticker": tickers,
        "market_cap": [random.randint(100000000, 1000000000000) for _ in range(n)],
        "exchange": [random.choice(exchanges) for _ in range(n)],
        "predicted_move": [random.uniform(-0.05, 0.05) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv("data/stock_data.csv", index=False)
    return df

# Streamlit app
st.title("Finespresso - Why it moves?")

# Email sign-up form
st.header("Sign up for alerts")
email = st.text_input("Enter your email:")
if st.button("Get my alerts"):
    if email:
        if save_email(email):
            st.success(f"Thank you! The daily Finespresso will be sent to {email}.")
        else:
            st.success("Thank you! We already have you covered for our daily Finespresso.")
    else:
        st.warning("Please enter a valid email address")

# Generate or load data
if not st.session_state.get("data_loaded"):
    df = generate_random_data()
    st.session_state.data_loaded = True
else:
    df = pd.read_csv("data/stock_data.csv")

# Create treemap
st.subheader("AI Predicted Moves Based on Newsflow")
fig = px.treemap(df, 
                 path=[px.Constant("All Stocks"), "exchange", "ticker"],
                 values="market_cap",
                 color="predicted_move",
                 hover_data=["market_cap", "predicted_move"],
                 color_continuous_scale="RdYlGn",
                 color_continuous_midpoint=0)

fig.update_layout(height=800)
st.plotly_chart(fig, use_container_width=True)
