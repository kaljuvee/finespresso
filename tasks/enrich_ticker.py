import logging
import time
from utils.db_util import get_news_without_tickers, update_news_tickers
from utils.openai_util import extract_ticker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_news_items(news_items):
    news_items_with_tickers = []
    for item in news_items:
        ticker = extract_ticker(item.title)
        news_items_with_tickers.append((item.id, ticker))
    return news_items_with_tickers

def main():
    start_time = time.time()
    logging.info("Starting ticker extraction task")
    
    news_without_tickers = get_news_without_tickers()
    if not news_without_tickers:
        logging.info("No news items without MW tickers found. Task completed.")
        return
    
    news_items_with_tickers = process_news_items(news_without_tickers)
    update_news_tickers(news_items_with_tickers)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Ticker extraction task completed. Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()