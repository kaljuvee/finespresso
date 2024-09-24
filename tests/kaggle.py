import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def scrape_kaggle_competitions(url, browser_type, ignore_https_errors):
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
        browser = await browser_launch.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=ignore_https_errors)
        page = await context.new_page()

        logging.info(f"Navigating to {url}")
        await page.goto(url, wait_until='networkidle', timeout=120000)  # Increased timeout to 2 minutes

        logging.info("Waiting for the page to load completely")
        try:
            # Wait for the site-container to be visible
            await page.wait_for_selector('#site-container', state='visible', timeout=120000)
            
            # Wait for dynamic content to load (adjust as needed)
            await page.wait_for_selector('div[role="main"]', state='visible', timeout=60000)
        except PlaywrightTimeoutError:
            logging.error("Timeout waiting for content to load")
            await browser.close()
            return pd.DataFrame()

        # Take a screenshot for debugging
        await page.screenshot(path="kaggle_page.png", full_page=True)
        logging.info("Screenshot saved as kaggle_page.png")

        # Save the page content for debugging
        content = await page.content()
        with open("kaggle_page_content.html", "w", encoding="utf-8") as f:
            f.write(content)
        logging.info("Page content saved as kaggle_page_content.html")

        logging.info("Attempting to extract competition data")
        try:
            competitions_data = await page.evaluate("""
                () => {
                    const competitions = [];
                    const items = document.querySelectorAll('div[role="main"] > div > div');
                    items.forEach(item => {
                        const titleElem = item.querySelector('h3');
                        const linkElem = item.querySelector('a');
                        const descriptionElem = item.querySelector('p');
                        const metaElems = item.querySelectorAll('span');
                        
                        if (titleElem && linkElem) {
                            competitions.push({
                                Title: titleElem.textContent.trim(),
                                Link: linkElem.href,
                                Description: descriptionElem ? descriptionElem.textContent.trim() : 'N/A',
                                Deadline: metaElems[0] ? metaElems[0].textContent.trim() : 'N/A',
                                Reward: metaElems[1] ? metaElems[1].textContent.trim() : 'N/A',
                                Team: metaElems[2] ? metaElems[2].textContent.trim() : 'N/A'
                            });
                        }
                    });
                    return competitions;
                }
            """)
        except Exception as e:
            logging.error(f"Error extracting competition data: {str(e)}")
            competitions_data = []

        await browser.close()
        
        df = pd.DataFrame(competitions_data)
        logging.info(f"Scraped {len(df)} Kaggle competitions")
        return df

async def main():
    parser = argparse.ArgumentParser(description="Web scraper for Kaggle Competitions")
    parser.add_argument("--url", default="https://www.kaggle.com/competitions?listOption=active", help="URL to scrape")
    parser.add_argument("--browser", default="firefox", choices=["chromium", "firefox", "webkit"], help="Browser to use")
    parser.add_argument("--ignore-https-errors", action="store_true", help="Ignore HTTPS errors")
    args = parser.parse_args()

    try:
        df = await scrape_kaggle_competitions(args.url, args.browser, args.ignore_https_errors)
        if not df.empty:
            print(df.head())
            df.to_csv('kaggle_competitions.csv', index=False)
            logging.info("Data saved to kaggle_competitions.csv")
        else:
            logging.warning("No data was scraped")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
