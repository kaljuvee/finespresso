import logging
import feedparser
import pandas as pd
from datetime import datetime
from utils.openai_util import summarize, tag_news
from utils.db_util import News, create_tables, add_news_items
from utils.tag_util import tags
from utils.web_util import fetch_url_content

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_date(date_string):
    # Add date parsing logic here. This is a placeholder.
    # You may need to adjust this based on the actual format of your date strings.
    return datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %z')

def parse_rss_feed(url, tags):
    logging.info(f"Parsing RSS feed from: {url}")
    feed = feedparser.parse(url)
    items = feed.entries[:100]  # Limit to 100 news items
    logging.info(f"Found {len(items)} items in the feed")

    data = []

    for index, item in enumerate(items, 1):
        logging.info(f"Processing item {index}/{len(items)}: {item.title}")
        
        title = item.title
        link = item.link
        pub_date = parse_date(item.published)
        company = item.get('issuer', 'N/A')
        content = fetch_url_content(link)
        logging.info(f"Fetched content for: {link}")

        try:
            ai_topic = tag_news(content, tags)
            logging.info(f"AI topic: {ai_topic}")
        except Exception as e:
            logging.warning(f"Error tagging news: {str(e)}")
            ai_topic = "Error in tagging"

        try:
            ai_summary = summarize(content)
            logging.info(f"Generated AI summary (first 50 chars): {ai_summary[:50]}...")
        except Exception as e:
            logging.warning(f"Error summarizing news: {str(e)}")
            ai_summary = "Error in summarization"

        data.append({
            'title': title,
            'link': link,
            'company': company,
            'published_date': pub_date,
            'ai_summary': ai_summary,
            'ai_topic': ai_topic,
            'source': 'Nasdaq Baltic',
            'industry': '',
            'publisher_topic': ''
        })

        logging.info(f"Added news item to dataframe: {title}")

    df = pd.DataFrame(data)
    logging.info(f"Created dataframe with {len(df)} rows")
    return df

def map_to_db(df):
    logging.info("Mapping dataframe to News objects")
    news_items = []
    for _, row in df.iterrows():
        news_item = News(
            title=row['title'],
            link=row['link'],
            company=row['company'],
            published_date=row['published_date'],
            ai_summary=row['ai_summary'],
            industry=row['industry'],
            publisher_topic=row['publisher_topic'],
            ai_topic=row['ai_topic'],
            source=row['source']
        )
        news_items.append(news_item)
    logging.info(f"Created {len(news_items)} News objects")
    return news_items

def write_to_csv(df, filename):
    logging.info(f"Writing dataframe to CSV: {filename}")
    df.to_csv(filename, index=False)
    logging.info("CSV file written successfully")

def main():
    logging.info("Starting main function")
    
    # Create tables
    logging.info("Creating tables")
    create_tables()
    
    # RSS feed URL
    rss_url = 'https://nasdaqbaltic.com/statistics/en/news?rss=1&num=100'
    logging.info(f"Using RSS feed URL: {rss_url}")

    # Fetch news and create dataframe
    logging.info("Fetching and parsing news items")
    news_df = parse_rss_feed(rss_url, tags)

    # Write dataframe to CSV
    csv_filename = 'news_items.csv'
    write_to_csv(news_df, csv_filename)

    # Map dataframe to News objects
    news_items = map_to_db(news_df)

    # Store news in the database
    logging.info(f"Adding {len(news_items)} news items to the database")
    add_news_items(news_items)
    
    logging.info("Main function completed")

if __name__ == '__main__':
    main()
