import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
from utils.db.news_db_util import map_to_db, add_news_items
from datetime import datetime, timedelta
import pytz
from tasks.enrich.enrich_ticker import process_publisher
from tasks.enrich.enrich_event import enrich_tag_from_url
from tasks.ai.predict import predict
from tasks.enrich.enrich_reason import enrich_reason

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
URL_PREFIX = 'https://live.euronext.com'
DEFAULT_URL = "https://live.euronext.com/en/products/equities/company-news"
DEFAULT_BROWSER = "firefox"

# Timezone mapping
TIMEZONE_MAPPING = {
    'CEST': 'Europe/Paris',
    'CET': 'Europe/Paris',
    'BST': 'Europe/London',
    'GMT': 'GMT',
}

async def scrape_euronext():
    async with async_playwright() as p:
        logging.info(f"Launching {DEFAULT_BROWSER} browser")
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()

        logging.info(f"Navigating to {DEFAULT_URL}")
        await page.goto(DEFAULT_URL)

        logging.info("Waiting for the news table to load")
        await page.wait_for_selector('table.table')

        logging.info("Extracting news data")
        news_data = []
        rows = await page.query_selector_all('table.table tbody tr')
        
        for row in rows:
            columns = await row.query_selector_all('td')
            if len(columns) >= 5:
                date = await columns[0].inner_text()
                company = await columns[1].inner_text()
                title_link = await columns[2].query_selector('a')
                title = await title_link.inner_text() if title_link else "N/A"
                link = await title_link.get_attribute('href') if title_link else "N/A"
                industry = await columns[3].inner_text()
                topic = await columns[4].inner_text()
                
                # Extract timezone and convert published_date to GMT
                try:
                    date_parts = date.split('\n')
                    if len(date_parts) == 2:
                        date_str, time_str = date_parts
                        time_parts = time_str.split()
                        if len(time_parts) == 2:
                            time, extracted_timezone = time_parts
                            date_str = f"{date_str} {time}"
                            local_dt = datetime.strptime(date_str, "%d %b %Y %H:%M")
                            timezone = TIMEZONE_MAPPING.get(extracted_timezone, 'UTC')
                        else:
                            raise ValueError("Unexpected time format")
                    else:
                        raise ValueError("Unexpected date format")
                except ValueError as e:
                    logging.error(f"Unable to parse date: {date}. Error: {str(e)}")
                    continue

                local_tz = pytz.timezone(timezone)
                local_dt = local_tz.localize(local_dt)
                gmt_dt = local_dt.astimezone(pytz.UTC)
                
                # Adjust GMT date if it's a future date
                current_time = datetime.now(pytz.UTC)
                if gmt_dt > current_time:
                    gmt_dt -= timedelta(days=1)
                
                news_data.append({
                    'published_date': local_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    'published_date_gmt': gmt_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    'company': company,
                    'title': title,
                    'link': URL_PREFIX + link,
                    'industry': industry,
                    'publisher_topic': topic,
                    'publisher': 'euronext',
                    'content': '',
                    'ticker': '',
                    'reason': '',  # Changed from ai_summary
                    'status': 'raw',
                    'timezone': timezone,
                    'publisher_summary': '',
                })

        await browser.close()
        
        df = pd.DataFrame(news_data)
        logging.info(f"Scraped {len(df)} news items")
        return df

async def main():
    try:
        df = await scrape_euronext()
        logging.info(f"Got {len(df)} rows from Euronext")
        logging.info(f"Sample data:\n{df.head()}")
        
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
        
        # Map dataframe to News objects
        news_items = map_to_db(df, 'euronext')

        # Store news in the database
        logging.info(f"Adding {len(news_items)} news items to the database")
        added_count, duplicate_count = add_news_items(news_items)
        logging.info(f"Euronext: added {added_count} news items to the database, {duplicate_count} duplicates skipped")

        # Call process_publisher after adding news items
        if added_count > 0:
            updated, skipped = process_publisher('euronext')
            logging.info(f"Euronext: Enriched {updated} items, skipped {skipped} items")
    except Exception as e:
        logging.error(f"Euronext: An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
