import feedparser
from datetime import datetime
from bs4 import BeautifulSoup
from utils.db_util import add_news_items, News
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_text(raw_html):
    return BeautifulSoup(raw_html, "lxml").text

def parse_date(date_string):
    try:
        return datetime.strptime(date_string, '%a, %d %b %Y %H:%M %Z')
    except ValueError:
        logger.warning(f"Unable to parse date: {date_string}. Using current time.")
        return datetime.utcnow()

def fetch_news(rss_url):
    feed = feedparser.parse(rss_url)
    news_items = []
    current_time = datetime.utcnow()

    for item in feed.entries:
        pub_date = parse_date(item.get('pubDate', '')) if 'pubDate' in item else current_time
        
        news_item = News(
            title=item.title,
            link=item.link,
            ai_summary=clean_text(item.description),
            published_date=pub_date,
            company=item.get('dc_contributor', 'N/A'),
            publisher='GlobeNewswire',
            industry=', '.join([
                kw.get('term') for kw in item.get('tags', [])
                if kw.get('scheme') == 'https://www.globenewswire.com/rss/keyword'
            ]),
            publisher_topic=item.get('dc_subject', 'N/A'),
            ticker=next((
                cat.get('term', 'N/A') for cat in item.get('tags', [])
                if cat.get('scheme') == 'https://www.globenewswire.com/rss/stock'
            ), 'N/A'),
            status='raw',
            downloaded_at=current_time
        )
        news_items.append(news_item)

    return news_items

def main():
    rss_url = 'https://www.globenewswire.com/RssFeed/country/United%20States/feedTitle/GlobeNewswire%20-%20News%20from%20United%20States'

    logger.info(f"Fetching news at {datetime.now()}")
    news_items = fetch_news(rss_url)
    add_news_items(news_items)
    logger.info(f"Added {len(news_items)} news items to the database.")

if __name__ == "__main__":
    main()
