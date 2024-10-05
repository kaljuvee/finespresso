import logging
from utils import news_db_util, price_move_util, price_move_db_util
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_price_move_task(publishers, days_back=7):
    logger.info(f"Starting price move task for publishers: {publishers}")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    logger.info(f"Date range: {start_date} to {end_date}")

    # Get news data for the specified publishers
    news_df = news_db_util.get_news_df(publishers, start_date, end_date)
    logger.info(f"Retrieved {len(news_df)} news items")

    for _, news_item in news_df.iterrows():
        try:
            news_id = news_item['news_id']
            ticker = news_item['ticker']
            published_date = news_item['published_date']

            if not all([news_id, ticker, published_date]):
                logger.error(f"Missing required fields in news item: {news_item}")
                continue

            logger.info(f"Processing news item: {news_id}")

            # Process the news item using price_move_util
            processed_row = price_move_util.set_prices(news_item)

            if processed_row['begin_price'] is None or processed_row['end_price'] is None:
                logger.warning(f"No price data available for {ticker} on {published_date}")
                continue

            # Create and store the PriceMove object
            try:
                price_move = price_move_util.create_price_move(
                    news_id=news_id,
                    ticker=ticker,
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
                logger.info(f"Processed and stored price move for news_id: {news_id}")
            except Exception as e:
                logger.error(f"Error creating or storing price move for news_id {news_id}: {str(e)}")
                logger.exception("Detailed traceback:")

        except Exception as e:
            logger.error(f"Error processing news item: {e}")
            logger.exception("Detailed traceback:")

    logger.info("Price move task completed")

if __name__ == "__main__":
    publishers = ["globenewswire_biotech"]  # Add your list of publishers
    run_price_move_task(publishers)
