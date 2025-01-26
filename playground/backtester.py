import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
import pytz
from utils.back_test_util import run_backtest
from utils.db.news_db_util import get_news_df_date_range
import pandas as pd
from utils.backtest_price_util import create_price_moves  # Update this import

# Time window configuration
TIME_WINDOW_DAYS = 5

# Calculate default dates
def get_default_dates():
    end_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=TIME_WINDOW_DAYS)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

# Backtest Parameters
START_DATE, END_DATE = get_default_dates()  # Default to last 25 days

# Trading Parameters
INITIAL_CAPITAL = 10000  # Changed from 100000 to 10000
POSITION_SIZE = 0.20    # 8% of capital per trade
TAKE_PROFIT = 0.01     # 1% take profit
STOP_LOSS = 0.005      # 0.5% stop loss

# Publisher Selection
PUBLISHERS = [
   # "globenewswire_country_fi",
    #"globenewswire_country_dk",
    "globenewswire_biotech",
    #"globenewswire_country_no",
    #"globenewswire_country_lt",
    #"globenewswire_country_lv",
    #"globenewswire_country_is",
    #"baltics",
    #"globenewswire_country_se",
    #"globenewswire_country_ee",
    #"omx",
    #"euronext"
]

# Event Selection (sorted by accuracy)
SELECTED_EVENTS = [
    "changes_in_companys_own_shares",  # 88.89%
    "business_contracts",              # 83.33%
    "patents",                         # 83.33%
    "shares_issue",                    # 81.82%
    "corporate_action",                # 81.82%
    "licensing_agreements",            # 80.00%
    "major_shareholder_announcements", # 75.00%
    "financial_results",               # 73.08%
    "financing_agreements",            # 71.43%
    "clinical_study",                  # 69.49%
    "dividend_reports_and_estimates",  # 66.67%
    "management_changes",              # 65.00%
    "partnerships",                    # 63.64%
    "earnings_releases_and_operating_result", # 61.54%
    "regulatory_filings",              # 61.54%
    "product_services_announcement"    # 60.00%
]

def run_backtest_from_parameters():
    # Convert dates to datetime with UTC timezone
    start_date = datetime.strptime(START_DATE, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d').replace(tzinfo=pytz.UTC)

    print(f"Running backtest from {start_date} to {end_date}")
    print(f"Using publishers: {PUBLISHERS}")
    print(f"Using events: {SELECTED_EVENTS}")
    print(f"Initial capital: {INITIAL_CAPITAL}")
    print(f"Position size: {POSITION_SIZE}")
    print(f"Take profit: {TAKE_PROFIT}")
    print(f"Stop loss: {STOP_LOSS}")

    # Get news data
    news_df = get_news_df_date_range(
        publishers=PUBLISHERS,
        start_date=start_date,
        end_date=end_date
    )

    # Filter by events
    if SELECTED_EVENTS:
        news_df = news_df[news_df['event'].isin(SELECTED_EVENTS)]

    # Run backtest
    results = run_backtest(
        news_df=news_df,
        initial_capital=INITIAL_CAPITAL,
        position_size=POSITION_SIZE,
        take_profit=TAKE_PROFIT,
        stop_loss=STOP_LOSS
    )

    if results is None:
        print("No trades were generated during the backtest period")
        return

    trades_df, metrics = results

    # Display metrics
    print("\nBacktest Results:")
    print(f"Total Return: {metrics['total_return']:.2f}%")
    print(f"Annualized Return: {metrics['annualized_return']:.2f}%")
    print(f"Total PnL: ${metrics['total_pnl']:,.2f}")
    print(f"Win Rate: {metrics['win_rate']:.2f}%")
    print(f"Total Trades: {metrics['total_trades']}")

    # Save trades to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"data/trades_{timestamp}.csv"
    os.makedirs("data", exist_ok=True)
    trades_df.to_csv(csv_path, index=False)
    print(f"\nTrade history saved to {csv_path}")

if __name__ == '__main__':
    run_backtest_from_parameters() 