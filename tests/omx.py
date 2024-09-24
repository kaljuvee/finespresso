import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def scrape_nasdaq_news(url, browser_type, ignore_https_errors):
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
                
                news_data.append({
                    'Date': date,
                    'Company': company,
                    'Category': category,
                    'Headline': headline,
                    'Link': link
                })

        await browser.close()
        
        df = pd.DataFrame(news_data)
        logging.info(f"Scraped {len(df)} news items")
        return df

async def main():
    parser = argparse.ArgumentParser(description="Web scraper for NASDAQ OMX Nordic Company News (First Page Only)")
    parser.add_argument("--url", default="https://www.nasdaqomxnordic.com/news/companynews", help="URL to scrape")
    parser.add_argument("--browser", default="firefox", choices=["chromium", "firefox", "webkit"], help="Browser to use")
    parser.add_argument("--ignore-https-errors", action="store_true", help="Ignore HTTPS errors")
    args = parser.parse_args()

    try:
        df = await scrape_nasdaq_news(args.url, args.browser, args.ignore_https_errors)
        print(df.head())
        df.to_csv('nasdaq_news_first_page.csv', index=False)
        logging.info("Data saved to nasdaq_news_first_page.csv")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
