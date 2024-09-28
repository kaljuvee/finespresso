import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import random

# Function to generate random data
def generate_random_data(n=100):
    tickers = [f"TICK{i:03d}" for i in range(1, n+1)]
    exchanges = ["NYSE", "NASDAQ", "LSE", "TSE", "HKEX"]
    data = {
        "ticker": tickers,
        "market_cap": [random.randint(100000000, 1000000000000) for _ in range(n)],
        "exchange": [random.choice(exchanges) for _ in range(n)],
        "predicted_move": [random.uniform(-0.05, 0.05) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv("stock_data.csv", index=False)
    return df

# Function to save email to DataFrame
def save_email(email):
    try:
        df = pd.read_csv("email_list.csv")
    except FileNotFoundError:
        df = pd.DataFrame(columns=["email", "captured_at"])
    
    new_row = pd.DataFrame({"email": [email], "captured_at": [datetime.now()]})
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv("email_list.csv", index=False)

# Streamlit app
st.title("Stock Market Insights")

# Email sign-up form
st.header("Sign up for alerts")
email = st.text_input("Enter your email:")
if st.button("Get my alerts"):
    if email:
        save_email(email)
        st.success(f"Thank you! Alerts will be sent to {email}")
    else:
        st.warning("Please enter a valid email address")

# Generate or load data
if not st.session_state.get("data_loaded"):
    df = generate_random_data()
    st.session_state.data_loaded = True
else:
    df = pd.read_csv("stock_data.csv")

# Create treemap
st.header("Stock Market Treemap")
fig = px.treemap(df, 
                 path=[px.Constant("All Stocks"), "exchange", "ticker"],
                 values="market_cap",
                 color="predicted_move",
                 hover_data=["market_cap", "predicted_move"],
                 color_continuous_scale="RdYlGn",
                 color_continuous_midpoint=0)

fig.update_layout(height=800)
st.plotly_chart(fig, use_container_width=True)
