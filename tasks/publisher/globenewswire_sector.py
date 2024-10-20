import feedparser
import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import argparse
import pandas as pd
from utils.db import news_db_util
from dateutil import parser as date_parser
import pytz
import logging
from utils.date.date_util import adjust_date_to_est

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sector = 'biotech'
# Initialize rss_dict as a global variable
rss_dict = {}

def load_config(sector):
    config_file = f"data/{sector}.json"
    try:
        with open(config_file, 'r') as file:
            rss_dict = json.load(file)
    except Exception as e:
        print(f"Error loading {config_file}: {e}")
        return None
    return rss_dict
        
def clean_text(raw_html):
    cleantext = BeautifulSoup(raw_html, "lxml").text
    return cleantext

def fetch_news(rss_dict, sector):
    all_news_items = []

    current_time = datetime.now(pytz.utc)
    print(f"Starting news fetch at {current_time}")
    for ticker, company_info in rss_dict.items():
        if 'url' not in company_info:
            print(f"Skipping ticker {ticker}: No RSS URL found")
            continue
        
        rss_url = company_info['url']
        company_name = company_info['company']
        
        print(f"Fetching news for ticker: {ticker} ({company_name})")
        feed = feedparser.parse(rss_url)

        for newsitem in feed['items']:
            last_subject = newsitem['tags'][-1]['term'] if 'tags' in newsitem and newsitem['tags'] else None
            try:
                published_date_gmt = date_parser.parse(newsitem['published'])
                adjusted_date = adjust_date_to_est(published_date_gmt)
            except ValueError:
                print(f"Warning: Unable to parse date '{newsitem['published']}' for ticker {ticker}. Skipping this news item.")
                continue

            all_news_items.append({
                'ticker': ticker,
                'title': newsitem['title'],
                'publisher_summary': clean_text(newsitem['summary']),
                'published_date_gmt': published_date_gmt,
                'published_date': adjusted_date,  
                'content': clean_text(newsitem['description']),
                'link': newsitem['link'],
                'company': company_name,
                'reason': '',  # Changed from ai_summary
                'industry': sector,
                'publisher_topic': last_subject,
                'event': '',  # Changed from ai_topic
                'publisher': f'globenewswire_{sector}',
                'downloaded_at': datetime.now(pytz.utc),
                'status': 'raw',
                'instrument_id': None,
                'yf_ticker': ticker,
                'timezone': 'US/Eastern',
                'ticker_url': '',
            })

    return pd.DataFrame(all_news_items)


def main(sector):
    print(f"Fetching news for sector: {sector}")
    
    rss_dict = load_config(sector)
    
    if rss_dict is None:
        print("Failed to load config.")
        return
        
    news_df = fetch_news(rss_dict, sector)
    
    # Map DataFrame to News objects
    news_items = news_db_util.map_to_db(news_df, f'globenewswire_{sector}')
    
    # Check for duplicates and add news items to the database
    added_count, duplicate_count = news_db_util.add_news_items(news_items)
    
    print(f"Added {added_count} news items to the database.")
    print(f"Skipped {duplicate_count} duplicate items.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch news for a specific sector.")
    parser.add_argument("-s", "--sector", default="biotech", help="Name of the sector. Default is 'biotech'")
    args = parser.parse_args()

    main(args.sector)
