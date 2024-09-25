import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import argparse
import logging
from utils.db_util import map_to_db, add_news_items
from utils.openai_util import summarize, tag_news

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
URL_PREFIX = 'https://live.euronext.com'

async def scrape_euronext(url, browser_type, ignore_https_errors):
    async with async_playwright() as p:
        browser_types = {
            'chromium': p.chromium,
            'firefox': p.firefox,
            'webkit': p.webkit
        }
        browser_launch = browser_types.get(browser_type.lower())
        if not browser_launch:
            raise ValueError(f"Invalid browser type: {browser_type}")

        logging.info(f"Launching {browser_type} browser")
        browser = await browser_launch.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=ignore_https_errors)
        page = await context.new_page()

        logging.info(f"Navigating to {url}")
        await page.goto(url)

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
                
                news_data.append({
                    'published_date': date,
                    'company': company,
                    'title': title,
                    'link': URL_PREFIX + link,
                    'industry': industry,
                    'publisher_topic': topic,
                    'publisher': 'Euronext'
                })

        await browser.close()
        
        df = pd.DataFrame(news_data)
        logging.info(f"Scraped {len(df)} news items")
        return df

async def main():
    parser = argparse.ArgumentParser(description="Web scraper for Euronext Company News")
    parser.add_argument("--url", default="https://live.euronext.com/en/products/equities/company-news", help="URL to scrape")
    parser.add_argument("--browser", default="firefox", choices=["chromium", "firefox", "webkit"], help="Browser to use")
    parser.add_argument("--ignore-https-errors", action="store_true", help="Ignore HTTPS errors")
    args = parser.parse_args()

    try:
        df = await scrape_euronext(args.url, args.browser, args.ignore_https_errors)
        print(df.head())
        df.to_csv('euronext_news.csv', index=False)
        logging.info("Data saved to euronext_news.csv")
        
        # Map dataframe to News objects
        news_items = map_to_db(df, 'euronext')

        # Store news in the database
        logging.info(f"Adding {len(news_items)} news items to the database")
        add_news_items(news_items)
        logging.info("Added news items to the database")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
