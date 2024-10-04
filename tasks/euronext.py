import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
from utils.db_util import map_to_db, add_news_items
from datetime import datetime
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
URL_PREFIX = 'https://live.euronext.com'
DEFAULT_URL = "https://live.euronext.com/en/products/equities/company-news"
DEFAULT_BROWSER = "firefox"
TIMEZONE = "CET"

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
                
                # Convert published_date to GMT
                try:
                    # Parse the new date format
                    date_parts = date.split('\n')
                    if len(date_parts) == 2:
                        date_str = f"{date_parts[0]} {date_parts[1].replace(' CEST', '')}"
                        local_dt = datetime.strptime(date_str, "%d %b %Y %H:%M")
                    else:
                        raise ValueError("Unexpected date format")
                except ValueError as e:
                    logging.error(f"Unable to parse date: {date}. Error: {str(e)}")
                    continue

                local_tz = pytz.timezone(TIMEZONE)
                local_dt = local_tz.localize(local_dt)
                gmt_dt = local_dt.astimezone(pytz.UTC)
                
                news_data.append({
                    'published_date': date,
                    'published_date_gmt': gmt_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    'company': company,
                    'title': title,
                    'link': URL_PREFIX + link,
                    'industry': industry,
                    'publisher_topic': topic,
                    'publisher': 'euronext',
                    'content': '',
                    'ticker': '',
                    'ai_summary': '',
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
        df = await scrape_euronext()
        logging.info(f"Got {len(df)} rows from Euronext")
        logging.info(f"Sample data:\n{df.head()}")
        
        # Map dataframe to News objects
        news_items = map_to_db(df, 'euronext')

        # Store news in the database
        logging.info(f"Adding {len(news_items)} news items to the database")
        add_news_items(news_items)
        logging.info("Euronext: added news items to the database")
    except Exception as e:
        logging.error(f"Euronext: An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
