import pandas as pd
from utils.instrument_db_util import save_instrument

def read_csv_to_df(file_path):
    return pd.read_csv(file_path)

def main():
    file_path = 'data/instrument_nasdaq_baltics.csv'
    df = read_csv_to_df(file_path)
    save_instrument(df)
    print("Instruments uploaded successfully.")

if __name__ == "__main__":
    main()
