import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from utils.db.news_db_util import map_to_db, add_news_items
from datetime import datetime
import pytz
from tasks.enrich.enrich_ticker import process_publisher
from utils.enrich_util import enrich_tag_from_url
from tasks.ai.predict import predict
from tasks.enrich.enrich_reason import enrich_reason
from utils.logging.log_util import get_logger
from utils.db.instrument_db_util import get_instrument_by_company_name

# Initialize logger
logger = get_logger(__name__)

DEFAULT_URL = "https://www.nasdaqomxnordic.com/news/companynews"
DEFAULT_BROWSER = "firefox"
TIMEZONE = "CET"

async def scrape_nasdaq_news():
    async with async_playwright() as p:
        logger.info(f"Launching {DEFAULT_BROWSER} browser")
        browser = await p.firefox.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()

        logger.info(f"Navigating to {DEFAULT_URL}")
        await page.goto(DEFAULT_URL)

        logger.info("Waiting for the news table to load")
        await page.wait_for_selector('#searchNewsTableId')

        logger.info("Extracting news data")
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
        logger.info(f"Scraped {len(df)} news items")
        return df

async def main():
    try:
        df = await scrape_nasdaq_news()
        logger.info(f"Got OMX dataframe with {len(df)} rows")
        logger.info(f"Sample data:\n{df.head()}")
        
        news_items = map_to_db(df, 'omx')
        logger.info(f"OMX: Processing all {len(news_items)} scraped items")

        # Convert news_items to DataFrame for further processing
        df = pd.DataFrame([item.__dict__ for item in news_items])

        # Enrich instrument ID
        logger.info("Starting instrument ID enrichment")
        for index, row in df.iterrows():
            instrument = get_instrument_by_company_name(row['company'])
            if instrument:
                df.at[index, 'instrument_id'] = instrument.id
                df.at[index, 'ticker'] = instrument.ticker
                df.at[index, 'yf_ticker'] = instrument.yf_ticker
                logger.info(f"Enriched instrument ID for {row['company']}: {instrument.id}")
            else:
                logger.info(f"No instrument found for company: {row['company']}")
        logger.info("Instrument ID enrichment completed")

        # 1. Enrich event
        try:
            df = enrich_tag_from_url(df)
            logger.info("Event enrichment completed successfully.")
            for index, row in df.iterrows():
                logger.info(f"Event for {row['link']}: {row['event']}")
        except Exception as e:
            logger.error(f"Error during event enrichment: {str(e)}", exc_info=True)
        
        # 2. Perform predictions
        try:
            df = predict(df)
            logger.info("Predictions completed successfully.")
        except Exception as e:
            logger.error(f"Error during predictions: {str(e)}", exc_info=True)
        
        # 3. Enrich reason
        try:
            df = enrich_reason(df)
            logger.info("Reason enrichment completed successfully.")
        except Exception as e:
            logger.error(f"Error during reason enrichment: {str(e)}", exc_info=True)
        
        # Map enriched DataFrame back to news items
        enriched_news_items = map_to_db(df, 'omx')

        # Add all items to the database without checking for duplicates
        added_count, _ = add_news_items(enriched_news_items, check_uniqueness=False)
        logger.info(f"OMX: added {added_count} news items to the database")

        # Call process_publisher after adding news items
        if added_count > 0:
            updated, skipped = process_publisher('omx')
            logger.info(f"OMX: Enriched {updated} items, skipped {skipped} items")
    except Exception as e:
        logger.error(f"OMX: An error occurred: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
