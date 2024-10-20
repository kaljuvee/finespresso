import logging
import os
from utils import price_move_util
from datetime import datetime, timedelta
import sys

from utils.db import news_db_util, price_move_db_util
# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/info.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
# List of publishers moved to the top
#PUBLISHERS = ["omx", "baltics", "euronext"]
PUBLISHERS = ["globenewswire_biotech"]
DAYS_BACK = 365

def test_price_move_task(news_id):
    logger.info(f"Starting test price move task for news_id: {news_id}")

    # Get news data for the specified news_id
    news_df = news_db_util.get_news_by_id(news_id)
    
    if news_df.empty:
        logger.error(f"No news item found with id: {news_id}")
        return

    logger.info(f"Retrieved news item with id: {news_id}")

    try:
        news_item = news_df.iloc[0]
        ticker = news_item['ticker']
        yf_ticker = news_item['yf_ticker']
        published_date = news_item['published_date']

        if not all([news_id, ticker, yf_ticker, published_date]):
            logger.error(f"Missing required fields in news item: {news_item}")
            return

        logger.info(f"Processing news item: {news_id} for ticker: {ticker} (YF: {yf_ticker})")

        # Process the news item using price_move_util
        processed_row = price_move_util.set_prices(news_item)

        if processed_row['begin_price'] is None or processed_row['end_price'] is None:
            logger.warning(f"No price data available for {yf_ticker} on {published_date}")
            return

        logger.info(f"Price data found for {yf_ticker}: Begin: {processed_row['begin_price']}, End: {processed_row['end_price']}")

        # Create and store the PriceMove object
        price_move = price_move_util.create_price_move(
            news_id=news_id,
            ticker=yf_ticker,
            published_date=published_date,
            begin_price=processed_row['begin_price'],
            end_price=processed_row['end_price'],
            index_begin_price=processed_row['index_begin_price'],
            index_end_price=processed_row['index_end_price'],
            volume=processed_row.get('Volume'),
            market=processed_row['market'],
            price_change=processed_row['price_change'],
            price_change_percentage=processed_row['price_change_percentage'],
            index_price_change=processed_row['index_price_change'],
            index_price_change_percentage=processed_row['index_price_change_percentage'],
            actual_side=processed_row['actual_side']
        )
        price_move_db_util.store_price_move(price_move)
        logger.info(f"Processed and stored price move for news_id: {news_id}, ticker: {yf_ticker}")

    except Exception as e:
        logger.error(f"Error processing news item: {e}")
        logger.exception("Detailed traceback:")

def run_price_move_task(publishers=PUBLISHERS, days_back=DAYS_BACK):
    logger.info(f"Starting price move task for publishers: {publishers}")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_BACK)
    logger.info(f"Date range: {start_date} to {end_date}")

    # Get news data for the specified publishers
    news_df = news_db_util.get_news_df(publishers, start_date, end_date)
    logger.info(f"Retrieved {len(news_df)} news items")

    successful_processes = 0
    failed_processes = 0

    for _, news_item in news_df.iterrows():
        try:
            news_id = news_item['news_id']
            ticker = news_item['ticker']
            yf_ticker = news_item['yf_ticker']
            published_date = news_item['published_date']

            if not all([news_id, ticker, yf_ticker, published_date]):
                logger.error(f"Missing required fields in news item: {news_item}")
                failed_processes += 1
                continue

            logger.info(f"Processing news item: {news_id} for ticker: {ticker} (YF: {yf_ticker})")

            # Process the news item using price_move_util
            processed_row = price_move_util.set_prices(news_item)

            if processed_row['begin_price'] is None or processed_row['end_price'] is None:
                logger.warning(f"No price data available for {yf_ticker} on {published_date}")
                failed_processes += 1
                continue

            logger.info(f"Price data found for {yf_ticker}: Begin: {processed_row['begin_price']}, End: {processed_row['end_price']}")

            # Create and store the PriceMove object
            price_move = price_move_util.create_price_move(
                news_id=news_id,
                ticker=yf_ticker,
                published_date=published_date,
                begin_price=processed_row['begin_price'],
                end_price=processed_row['end_price'],
                index_begin_price=processed_row['index_begin_price'],
                index_end_price=processed_row['index_end_price'],
                volume=processed_row.get('Volume'),
                market=processed_row['market'],
                price_change=processed_row['price_change'],
                price_change_percentage=processed_row['price_change_percentage'],
                index_price_change=processed_row['index_price_change'],
                index_price_change_percentage=processed_row['index_price_change_percentage'],
                actual_side=processed_row['actual_side']
            )
            price_move_db_util.store_price_move(price_move)
            logger.info(f"Processed and stored price move for news_id: {news_id}, ticker: {yf_ticker}")
            successful_processes += 1

        except Exception as e:
            logger.error(f"Error processing news item: {e}")
            logger.exception("Detailed traceback:")
            failed_processes += 1

    logger.info(f"Price move task completed. Successful: {successful_processes}, Failed: {failed_processes}")
    logger.info(f"Total news items: {len(news_df)}, Processed: {successful_processes + failed_processes}")

if __name__ == "__main__":
    # Comment out the following line during testing:
    run_price_move_task()
    
    # Uncomment the following line and replace NEWS_ID with the actual news_id you want to test:
    #news_id = 15820
    #test_price_move_task(news_id)