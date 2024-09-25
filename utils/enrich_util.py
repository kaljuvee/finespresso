import logging
import pandas as pd
from utils.web_util import fetch_url_content
from utils.openai_util import summarize, tag_news
from utils.tag_util import tags

def enrich(df):
    logging.info("Starting enrichment process")
    
    def enrich_row(row):
        link = row['link']
        
        # Fetch content
        try:
            content = fetch_url_content(link)
            logging.info(f"Fetched content for: {link}")
        except Exception as e:
            logging.error(f"Error fetching content for {link}: {e}")
            content = ""

        # Tag news
        try:
            ai_topic = tag_news(content, tags)
            logging.info(f"AI topic for {link}: {ai_topic}")
        except Exception as e:
            logging.error(f"Error tagging news for {link}: {e}")
            ai_topic = "Error in tagging"

        # Summarize news
        try:
            ai_summary = summarize(content)
            logging.info(f"Generated AI summary for {link} (first 50 chars): {ai_summary[:50]}...")
        except Exception as e:
            logging.error(f"Error summarizing news for {link}: {e}")
            ai_summary = "Error in summarization"

        return pd.Series({
            'content': content,
            'ai_topic': ai_topic,
            'ai_summary': ai_summary
        })

    # Apply the enrichment to each row
    logging.info("Applying enrichment to each news item")
    enriched_data = df.apply(enrich_row, axis=1)
    
    # Combine the original DataFrame with the enriched data
    enriched_df = pd.concat([df, enriched_data], axis=1)
    
    logging.info(f"Enrichment completed. DataFrame now has {len(enriched_df.columns)} columns")
    
    return enriched_df

# Example usage:
# enriched_df = enrich(original_df)