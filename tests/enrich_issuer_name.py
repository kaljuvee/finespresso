import pandas as pd
import yfinance as yf

# Load your CSV file
df = pd.read_csv("data/instrument_nasdaq_semiconductors.csv")

# Fetch company names for each ticker using yfinance
def fetch_company_name(ticker):
    try:
        stock = yf.Ticker(ticker)
        return stock.info['longName']
    except KeyError:
        return "Unknown"

# Apply function to populate the issuer column
df["issuer"] = df["ticker"].apply(fetch_company_name)

# Generate the Google Finance URL
df["url"] = df["ticker"].apply(lambda ticker: f"https://www.google.com/finance/quote/{ticker}:NASDAQ")

# Save the updated DataFrame to a new CSV file
df.to_csv("data/enriched_instrument_nasdaq_semiconductors.csv", index=False)
