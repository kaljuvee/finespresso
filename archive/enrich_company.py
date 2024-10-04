import logging
from utils.db_util import get_news_without_company, update_companies
from utils.openai_util import extract_issuer
import pandas as pd
import time
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of publishers
PUBLISHERS = ['globenewswire']

def news_to_dataframe(news_items):
    logging.info("Converting news items to DataFrame")
    df = pd.DataFrame([{'id': item.id, 'link': item.link} for item in news_items])
    logging.info(f"Created DataFrame with {len(df)} rows")
    return df

def fetch_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup.get_text()
    except Exception as e:
        logging.error(f"Error fetching content from {url}: {str(e)}")
        return None

def enrich_company_from_url(df):
    logging.info("Enriching news items with company names")
    enriched_data = []
    for _, row in df.iterrows():
        content = fetch_content(row['link'])
        logging.info(f'Enriching company from url: {row["link"]}')
        if content:
            try:
                company = extract_issuer(content)
                print('Enriched company from url', company)
                logging.info(f'Enriched company from url: {company}')
                enriched_data.append({'id': row['id'], 'company': company})
            except Exception as e:
                logging.error(f"Error extracting company for {row['link']}: {str(e)}")
    
    enriched_df = pd.DataFrame(enriched_data)
    logging.info(f"Enriched {len(enriched_df)} news items with company names")
    return enriched_df

def process_publisher(publisher):
    logging.info(f"Processing publisher: {publisher}")
    
    news_without_company = get_news_without_company(publisher)
    if not news_without_company:
        logging.info(f"No news items without company names found for {publisher}. Skipping.")
        return
    
    news_df = news_to_dataframe(news_without_company)
    
    logging.info(f"Enriching news items with company names for {publisher}")
    enriched_df = enrich_company_from_url(news_df)
    
    update_companies(enriched_df)

def main():
    start_time = time.time()
    logging.info("Starting company name enrichment task")
    
    for publisher in PUBLISHERS:
        process_publisher(publisher)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Company name enrichment task completed for all publishers. Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()