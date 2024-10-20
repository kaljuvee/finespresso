import logging
import pandas as pd
from utils.ai.openai_util import enrich_reason as openai_enrich_reason
from utils.db.news_db_util import get_news_df, update_records

def enrich_reason(df):
    logging.info("Starting summary enrichment process")
    def apply_summary(row):
        try:
            text = row.get('content') or row.get('title')
            if not text:
                logging.warning(f"Skipping record for {row.get('link', 'unknown link')}: No content or title found")
                return None
            
            reason = openai_enrich_reason(text, row.get('predicted_move'))
            logging.info(f"Generated AI summary for {row['link']} (first 50 chars): {reason[:50]}...")
            return reason
        except Exception as e:
            logging.error(f"Error summarizing news for {row['link']}: {e}")
            return "Error in summarization"
    
    df['reason'] = df.apply(apply_summary, axis=1)
    logging.info(f"Summary enrichment completed for {len(df)} items")
    return df

def main():
    logging.basicConfig(level=logging.INFO)
    
    # Get all news from the database
    df = get_news_df()
    
    # Filter for rows without a reason
    df_to_enrich = df[df['predicted_move'].isnull()]
    
    enriched_df = enrich_reason(df_to_enrich)
    
    # Print statistics
    total_rows = len(df)
    enriched_rows = len(enriched_df)
    successful_enrichments = enriched_df['reason'].notna().sum()
    
    logging.info(f"Total news items: {total_rows}")
    logging.info(f"Processed news items: {enriched_rows}")
    logging.info(f"Successfully enriched items: {successful_enrichments}")
    logging.info(f"Percentage successfully enriched: {(successful_enrichments / total_rows) * 100:.2f}%")
    
    # Update the database with all processed records, including those that failed enrichment
    update_records(enriched_df)

if __name__ == "__main__":
    main()
