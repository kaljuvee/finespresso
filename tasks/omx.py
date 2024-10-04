import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging
from utils.db_util import map_to_db, add_news_items
from datetime import datetime
import pytz

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
                    'ai_summary': '',
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
        
        news_items = map_to_db(df, 'omx')

        add_news_items(news_items)
        logging.info(f"OMX: added {len(news_items)} news items to the database")
    except Exception as e:
        logging.error(f"OMX: An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
