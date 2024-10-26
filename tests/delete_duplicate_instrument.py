import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db.instrument_db_util import get_all_instruments, delete_instruments
from utils.logging.log_util import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting duplicate instrument deletion process")

    # Get all instruments
    df = get_all_instruments()
    logger.info(f"Retrieved {len(df)} instruments from the database")

    # Check for duplicate 'ticker' fields
    duplicates = df[df.duplicated(subset=['ticker'], keep=False)]
    
    if duplicates.empty:
        logger.info("No duplicate instruments found")
        return

    logger.info(f"Found {len(duplicates)} duplicate instruments")

    # Group by ticker and keep the highest (most recent) ID
    to_keep = duplicates.groupby('ticker')['id'].max()
    to_delete = duplicates[~duplicates['id'].isin(to_keep)]

    if to_delete.empty:
        logger.info("No instruments to delete after keeping the most recent")
        return

    # Delete duplicates
    delete_ids = to_delete['id'].tolist()
    delete_instruments(delete_ids)

    logger.info(f"Deleted {len(delete_ids)} duplicate instruments")

if __name__ == "__main__":
    main()

