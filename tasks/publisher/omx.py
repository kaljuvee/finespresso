import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
from utils.db.news_db_util import map_to_db, add_news_items, remove_duplicates
from datetime import datetime
import pytz
from tasks.enrich.enrich_ticker import process_publisher
from tasks.enrich.enrich_event import enrich_tag_from_url
from tasks.ai.predict import predict
from tasks.enrich.enrich_reason import enrich_reason

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

DEFAULT_URL = "https://www.nasdaqomxnordic.com/news/companynews"
DEFAULT_BROWSER = "firefox"
TIMEZONE = "CET"

async def scrape_nasdaq_news():
    async with async_playwright() as p:
        logging.info(f"Launching {DEFAULT_BROWSER} browser")
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()

        logging.info(f"Navigating to {DEFAULT_URL}")
        await page.goto(DEFAULT_URL)

        logging.info("Waiting for the news table to load")
        await page.wait_for_selector('#searchNewsTableId')

        logging.info("Extracting news data")
        news_data = []
        rows = await page.query_selector_all('#searchNewsTableId tbody tr')
        
        for row in rows:
            columns = await row.query_selector_all('td')
            if len(columns) >= 5:
                date = await columns[0].inner_text()
                company = await columns[1].inner_text()
                category = await columns[2].inner_text()
                headline_link = await columns[3].query_selector('a')
                headline = await headline_link.inner_text() if headline_link else "N/A"
                link = await headline_link.get_attribute('href') if headline_link else "N/A"
                
                # Convert published_date to GMT
                local_dt = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                local_tz = pytz.timezone(TIMEZONE)
                local_dt = local_tz.localize(local_dt)
                gmt_dt = local_dt.astimezone(pytz.UTC)
                
                news_data.append({
                    'published_date': date,
                    'published_date_gmt': gmt_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    'company': company,
                    'title': headline,
                    'link': link,
                    'publisher_topic': category,
                    'content': '',
                    'ticker': '',
                    'reason': '',  # Changed from ai_summary
                    'industry': '',
                    'publisher': 'omx',
                    'status': 'raw',
                    'timezone': TIMEZONE,
                    'publisher_summary': '',
                })

        await browser.close()
        
        df = pd.DataFrame(news_data)
        logging.info(f"Scraped {len(df)} news items")
        return df

async def main():
    try:
        df = await scrape_nasdaq_news()
        logging.info(f"Got OMX dataframe with {len(df)} rows")
        logging.info(f"Sample data:\n{df.head()}")
        
        # Perform duplicate removal directly on the DataFrame
        news_items = map_to_db(df, 'omx')
        unique_items, duplicate_count = remove_duplicates(news_items)
        logging.info(f"OMX: {duplicate_count} duplicate news items removed")

        # Convert unique_items back to DataFrame for further processing
        df = pd.DataFrame([{
            'title': item.title,
            'link': item.link,
            'company': item.company,
            'published_date': item.published_date,
            'content': item.content,
            'reason': item.reason,
            'industry': item.industry,
            'publisher_topic': item.publisher_topic,
            'event': item.event,
            'publisher': item.publisher,
            'status': item.status,
            'instrument_id': item.instrument_id,
            'yf_ticker': item.yf_ticker,
            'ticker': item.ticker,
            'published_date_gmt': item.published_date_gmt,
            'timezone': item.timezone,
            'publisher_summary': item.publisher_summary,
            'ticker_url': item.ticker_url,
            'predicted_side': item.predicted_side,
            'predicted_move': item.predicted_move
        } for item in unique_items])

        # 1. Enrich event
        try:
            df['event'] = None  # Ensure 'event' column exists
            df = enrich_tag_from_url(df)
            logging.info("Event enrichment completed successfully.")
        except Exception as e:
            logging.error(f"Error during event enrichment: {str(e)}", exc_info=True)
        
        # 2. Perform predictions
        try:
            df = predict(df)
            logging.info("Predictions completed successfully.")
        except Exception as e:
            logging.error(f"Error during predictions: {str(e)}", exc_info=True)
        
        # 3. Enrich reason
        try:
            df = enrich_reason(df)
            logging.info("Reason enrichment completed successfully.")
        except Exception as e:
            logging.error(f"Error during reason enrichment: {str(e)}", exc_info=True)
        
        # Map enriched DataFrame back to news items
        enriched_news_items = map_to_db(df, 'omx')

        added_count, duplicate_count = add_news_items(enriched_news_items)
        logging.info(f"OMX: added {added_count} news items to the database, {duplicate_count} duplicates skipped")

        # Call process_publisher after adding news items
        if added_count > 0:
            updated, skipped = process_publisher('omx')
            logging.info(f"OMX: Enriched {updated} items, skipped {skipped} items")
    except Exception as e:
        logging.error(f"OMX: An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
