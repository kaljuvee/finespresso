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
        browser = await browser_launch.launch(headless=False)  # Changed to headless=False for debugging
        context = await browser.new_context(ignore_https_errors=ignore_https_errors)
        page = await context.new_page()

        logging.info(f"Navigating to {url}")
        await page.goto(url, wait_until='networkidle', timeout=60000)

        logging.info("Waiting for the page to load completely")
        await page.wait_for_load_state('networkidle', timeout=60000)

        # Take a screenshot for debugging
        await page.screenshot(path="kaggle_page.png", full_page=True)
        logging.info("Screenshot saved as kaggle_page.png")

        # Save the page content for debugging
        content = await page.content()
        with open("kaggle_page_content.html", "w", encoding="utf-8") as f:
            f.write(content)
        logging.info("Page content saved as kaggle_page_content.html")

        logging.info("Checking for competition elements")
        selectors = ['.competition-row', '.competition-list-view__item', '.c-list-view__item', 'div[data-component-name="CompetitionsList"]']
        competition_selector = None
        for selector in selectors:
            elements = await page.query_selector_all(selector)
            if elements:
                competition_selector = selector
                logging.info(f"Found {len(elements)} elements with selector: {selector}")
                break

        if not competition_selector:
            logging.error("Could not find any competition elements")
            await browser.close()
            return pd.DataFrame()

        logging.info("Extracting competition data")
        competitions_data = []
        
        competition_rows = await page.query_selector_all(competition_selector)
        for row in competition_rows:
            title_elem = await row.query_selector('h3, .competition-title')
            title = await title_elem.inner_text() if title_elem else "N/A"
            
            link_elem = await row.query_selector('a')
            link = await link_elem.get_attribute('href') if link_elem else "N/A"
            
            description_elem = await row.query_selector('p, .competition-description')
            description = await description_elem.inner_text() if description_elem else "N/A"
            
            deadline_elem = await row.query_selector('.competition-deadline, .c-list-view__meta')
            deadline = await deadline_elem.inner_text() if deadline_elem else "N/A"
            
            reward_elem = await row.query_selector('.competition-reward')
            reward = await reward_elem.inner_text() if reward_elem else "N/A"
            
            team_elem = await row.query_selector('.competition-team')
            team = await team_elem.inner_text() if team_elem else "N/A"
            
            competitions_data.append({
                'Title': title,
                'Link': f"https://www.kaggle.com{link}" if link != "N/A" else "N/A",
                'Description': description,
                'Deadline': deadline,
                'Reward': reward,
                'Team': team
            })

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
