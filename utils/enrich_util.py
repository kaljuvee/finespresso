import logging
import pandas as pd
from utils.scrape.web_util import fetch_url_content
from utils.ai.openai_util import enrich_reason, tag_news
from utils.static.tag_util import tags, tag_list
import re


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def enrich_tag_from_url(df):
    print("Starting enrichment process from URLs")
    logging.info("Starting enrichment process from URLs")
    
    def fetch_and_tag(row):
        try:
            content = fetch_url_content(row['link'])
            ai_topic = tag_news(content, tags)
            print(f"Generated tag for: {row['link']} - Tag: {ai_topic}")
            logging.info(f"Generated tag for: {row['link']} - Tag: {ai_topic}")
            return ai_topic
        except Exception as e:
            print(f"Error processing {row['link']}: {str(e)}")
            logging.error(f"Error processing {row['link']}: {str(e)}")
            return None
    
    df['ai_topic'] = df.apply(fetch_and_tag, axis=1)
    print(f"Enrichment completed for {len(df)} items")
    logging.info(f"Enrichment completed for {len(df)} items")
    return df

def enrich_reason_from_url(df):
    logging.info("Starting enrichment process from URLs")
    def fetch_and_summarize(row):
        try:
            content = fetch_url_content(row['link'])
            reason = enrich_reason(content, row['predicted_move'])  # Changed from ai_summary
            logging.info(f"Generated reason for: {row['link']} (first 50 chars): {reason[:50]}...")
            return reason
        except Exception as e:
            logging.error(f"Error processing {row['link']}: {str(e)}")
            return None
    
    df['reason'] = df.apply(fetch_and_summarize, axis=1)  # Changed from ai_summary
    logging.info(f"Enrichment completed for {len(df)} items")
    return df

def enrich_from_content(df):
    logging.info("Starting enrichment process from existing content")

    def apply_tag(row):
        try:
            if pd.notna(row['content']) and row['content']:
                ai_topic = tag_news(row['content'], tags)
                logging.info(f"AI topic for {row['link']}: {ai_topic}")
                return ai_topic
            else:
                logging.warning(f"No content available for tagging: {row['link']}")
                return "No content available for tagging"
        except Exception as e:
            logging.error(f"Error tagging news for {row['link']}: {str(e)}")
            return f"Error in tagging: {str(e)}"

    def apply_summary(row):
        try:
            if pd.notna(row['content']) and row['content']:
                reason = enrich_reason(row['content'], row['predicted_move'])  # Changed from ai_summary
                logging.info(f"Generated reason for {row['link']} (first 50 chars): {reason[:50]}...")
                return reason
            else:
                logging.warning(f"No content available for summarization: {row['link']}")
                return "No content available for summarization"
        except Exception as e:
            logging.error(f"Error summarizing news for {row['link']}: {str(e)}")
            return f"Error in summarization: {str(e)}"

    df['ai_topic'] = df.apply(apply_tag, axis=1)
    df['reason'] = df.apply(apply_summary, axis=1)  # Changed from ai_summary
    logging.info(f"Enrichment from content completed for {len(df)} items")
    return df

def enrich_content_from_url(df):
    logging.info("Starting content enrichment from URLs")
    
    def fetch_and_enrich(row):
        try:
            content = fetch_url_content(row['link'])
            logging.info(f"Enriched content for: {row['link']}")
            print(f"Enriched content for: {row['link']}")
            return pd.Series({'content': content})  # Changed from ai_summary
        except Exception as e:
            logging.error(f"Error processing {row['link']}: {str(e)}")
            return pd.Series({'content': None, 'reason': None})  # Changed from ai_summary
    
    enriched = df.apply(fetch_and_enrich, axis=1)
    df = pd.concat([df, enriched], axis=1)
    logging.info(f"Content enrichment completed for {len(df)} items")
    return df

def determine_event_from_content(content):
    content = content.lower()
    
    for event in tag_list:
        # Convert event to lowercase and replace underscores with spaces for matching
        event_keywords = event.lower().replace('_', ' ').split()
        if all(keyword in content for keyword in event_keywords):
            return event
    
    return None  # If no event is detected
