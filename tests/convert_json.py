import csv

# Load JSON data
data = {
    "FBIO": {"company": "Fortress Biotech"},
    "KA": {"company": "Kineta"},
    "QGEN": {"company": "QIAGEN"},
    "DYAI": {"company": "Dyadic International"},
    "CKPT": {"company": "Checkpoint Therapeutics"},
}

# Define CSV headers and file path
headers = ["issuer", "ticker", "yf_ticker", "isin", "asset_class", "exchange", "exchange_code", "country", "url"]
file_path = "/mnt/data/biotech_data.csv"

# Generate CSV data
with open(file_path, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()

    for ticker, info in data.items():
        writer.writerow({
            "issuer": info["company"],
            "ticker": ticker,
            "yf_ticker": ticker,
            "isin": "",  # Placeholder, as ISIN is not provided
            "asset_class": "equity",
            "exchange": "nasdaq_us",
            "exchange_code": "NASDAQ",
            "country": "US",
            "url": f"https://www.google.com/finance/quote/{ticker}:NASDAQ"
        })