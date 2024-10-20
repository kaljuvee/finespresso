import logging
import feedparser
import pandas as pd
from datetime import datetime
import pytz
from utils.db.news_db_util import add_news_items, map_to_db
from utils.static.tag_util import tags
from tasks.enrich.enrich_ticker import process_publisher

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TIMEZONE = "EET"

def parse_date(date_string):
    logging.debug(f"Parsing date: {date_string}")
    try:
        return datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %z')
    except ValueError as e:
        logging.error(f"Error parsing date: {e}")
        return None

def parse_rss_feed(url, tags):
    logging.info(f"Parsing RSS feed from: {url}")
    try:
        feed = feedparser.parse(url) 
    except Exception as e:
        logging.error(f"Error parsing RSS feed: {e}")
        return pd.DataFrame()

    items = feed.entries[:100]  # Limit to 100 news items
    logging.info(f"Found {len(items)} items in the feed")

    data = []

    for index, item in enumerate(items, 1):
        logging.debug(f"Processing item {index}/{len(items)}: {item.title}")
        
        title = item.title
        link = item.link
        pub_date = parse_date(item.published)
        company = item.get('issuer', 'N/A')
        
        # Convert published_date to GMT
        if pub_date:
            gmt_dt = pub_date.astimezone(pytz.UTC)
            pub_date_str = pub_date.strftime("%Y-%m-%d %H:%M:%S")
            pub_date_gmt_str = gmt_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            pub_date_str = pub_date_gmt_str = None
        
        data.append({
            'title': title,
            'link': link,
            'company': company,
            'published_date': pub_date_str,
            'published_date_gmt': pub_date_gmt_str,
            'publisher': 'baltics',
            'industry': '',
            'content': '',
            'ticker': '',
            'reason': '',  # Changed from ai_summary
            'publisher_topic': '',
            'status': 'raw',
            'timezone': TIMEZONE,
            'publisher_summary': '',
            'ticker_url': '',  # Add this line to include ticker_url
            'event': '',  # Changed from ai_summary
        })

        logging.debug(f"Added news item to dataframe: {title}")

    df = pd.DataFrame(data)
    logging.info(f"Created dataframe with {len(df)} rows")
    return df

def main():
    logging.info("Starting main function")
    
    try:
        # RSS feed URL
        rss_url = 'https://nasdaqbaltic.com/statistics/en/news?rss=1&num=100'
        logging.info(f"Using RSS feed URL: {rss_url}")

        # Fetch news and create dataframe
        logging.info("Fetching and parsing news items")
        news_df = parse_rss_feed(rss_url, tags)
        logging.info(f"Created dataframe with {len(news_df)} rows")
        # Map dataframe to News objects
        news_items = map_to_db(news_df, 'baltics')

        # Store news in the database
        added_count, duplicate_count = add_news_items(news_items)
        logging.info(f"Nasdaq Baltics: added {added_count} news items to the database, {duplicate_count} duplicates skipped")

        # Call process_publisher after adding news items
        if added_count > 0:
            updated, skipped = process_publisher('baltics')
            logging.info(f"Nasdaq Baltics: Enriched {updated} items, skipped {skipped} items")

    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == '__main__':
    main()
