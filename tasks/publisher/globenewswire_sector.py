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
from tasks.enrich.enrich_event import enrich_tag_from_url
from tasks.ai.predict import predict
from tasks.enrich.enrich_reason import enrich_reason

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
    
    # Check for duplicates before enrichment
    news_items = news_db_util.map_to_db(news_df, f'globenewswire_{sector}')
    
    session = news_db_util.Session()
    try:
        unique_news_items, duplicate_count = news_db_util.remove_duplicates(session, news_items)
        
        if not unique_news_items:
            print("No new news items found. Exiting.")
            return
        
        # Convert News objects back to a DataFrame
        unique_news_df = pd.DataFrame([
            {column.name: getattr(item, column.name) for column in news_db_util.News.__table__.columns}
            for item in unique_news_items
        ])
        
        # Proceed with enrichment only for unique items
        try:
            unique_news_df['event'] = None  # Ensure 'event' column exists
            unique_news_df = enrich_tag_from_url(unique_news_df)
            print("Event enrichment completed successfully.")
        except Exception as e:
            print(f"Error during event enrichment: {str(e)}")
            logging.error(f"Event enrichment failed: {str(e)}", exc_info=True)
        
        try:
            unique_news_df = predict(unique_news_df)
            print("Predictions completed successfully.")
        except Exception as e:
            print(f"Error during predictions: {str(e)}")
            logging.error(f"Predictions failed: {str(e)}", exc_info=True)
        
        try:
            unique_news_df = enrich_reason(unique_news_df)
            print("Reason enrichment completed successfully.")
        except Exception as e:
            print(f"Error during reason enrichment: {str(e)}")
            logging.error(f"Reason enrichment failed: {str(e)}", exc_info=True)
        
        # Map enriched DataFrame back to News objects
        enriched_news_items = news_db_util.map_to_db(unique_news_df, f'globenewswire_{sector}')
        
        # Add enriched news items to the database
        session.add_all(enriched_news_items)
        session.commit()
        
        print(f"Added {len(enriched_news_items)} news items to the database.")
        print(f"Skipped {duplicate_count} duplicate items.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    sector = 'biotech'
    main(sector)
