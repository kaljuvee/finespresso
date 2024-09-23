from flask import Flask
from flask_apscheduler import APScheduler
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

async def scrape_nasdaq_nordic(url="https://www.nasdaqomxnordic.com/news/companynews", browser_type="chromium"):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url)
        
        await page.wait_for_selector('#searchNewsTableId')
        
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
        df.to_csv('nasdaq_nordic_news.csv', index=False)
        logging.info(f"Scraped {len(df)} NASDAQ Nordic news items")

async def scrape_euronext(url="https://live.euronext.com/en/products/equities/company-news", browser_type="chromium"):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url)
        
        await page.wait_for_selector('table.table')
        
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
                    'Date': date,
                    'Company': company,
                    'Title': title,
                    'Link': link,
                    'Industry': industry,
                    'Topic': topic
                })
        
        await browser.close()
        
        df = pd.DataFrame(news_data)
        df.to_csv('euronext_news.csv', index=False)
        logging.info(f"Scraped {len(df)} Euronext news items")

def run_nasdaq_nordic_scraper():
    asyncio.run(scrape_nasdaq_nordic())

def run_euronext_scraper():
    asyncio.run(scrape_euronext())

# Schedule jobs
scheduler.add_job(id='scrape_nasdaq_nordic', func=run_nasdaq_nordic_scraper, trigger='interval', hours=6)
scheduler.add_job(id='scrape_euronext', func=run_euronext_scraper, trigger='interval', hours=6)

@app.route('/')
def home():
    return "Scraper is running. Check logs for details."

if __name__ == '__main__':
    app.run(debug=True)
