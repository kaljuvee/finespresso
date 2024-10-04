import feedparser
import json
from bs4 import BeautifulSoup
from datetime import datetime
import argparse
import pandas as pd
from utils import db_util
from dateutil import parser as date_parser
from pytz import timezone

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

    current_time = datetime.now()
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
                # Convert GMT to EST
                est_tz = timezone('US/Eastern')
                published_date_est = published_date_gmt.astimezone(est_tz)
            except ValueError:
                print(f"Warning: Unable to parse date '{newsitem['published']}' for ticker {ticker}. Skipping this news item.")
                continue
            all_news_items.append({
                'ticker': ticker,
                'title': newsitem['title'],
                'publisher_summary': clean_text(newsitem['summary']),
                'published_date_gmt': published_date_gmt,
                'published_date': published_date_est,
                'content': clean_text(newsitem['description']),
                'link': newsitem['link'],
                'language': newsitem.get('dc_language', None),
                'publisher_topic': last_subject,
                'industry': sector,
                'company': company_name,
                'status': 'raw',
                'publisher': f'globenewswire_{sector}',  # Updated this line
                'timezone': 'EST',
                'ai_summary': '',

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
    news_items = db_util.map_to_db(news_df, 'globenewswire')
    
    # Add news items to the database
    db_util.add_news_items(news_items)
    
    print(f"Added {len(news_items)} news items to the database.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch news for a specific sector.")
    parser.add_argument("-s", "--sector", default="biotech", help="Name of the sector. Default is 'biotech'")
    args = parser.parse_args()

    main(args.sector)