import logging
import pandas as pd
from utils.web_util import fetch_url_content
from utils.openai_util import summarize, tag_news
from utils.tag_util import tags

def enrich_content(df):
    logging.info("Starting content enrichment process")
    def fetch_content(row):
        try:
            content = fetch_url_content(row['link'])
            logging.info(f"Fetched content for: {row['link']}")
            return content
        except Exception as e:
            logging.error(f"Error fetching content for {row['link']}: {e}")
            return ""
    
    df['content'] = df.apply(fetch_content, axis=1)
    logging.info(f"Content enrichment completed for {len(df)} items")
    return df

def enrich_tags(df):
    logging.info("Starting tag enrichment process")
    def apply_tag(row):
        try:
            ai_topic = tag_news(row['content'], tags)
            logging.info(f"AI topic for {row['link']}: {ai_topic}")
            return ai_topic
        except Exception as e:
            logging.error(f"Error tagging news for {row['link']}: {e}")
            return "Error in tagging"
    
    df['ai_topic'] = df.apply(apply_tag, axis=1)
    logging.info(f"Tag enrichment completed for {len(df)} items")
    return df

def enrich_summary(df):
    logging.info("Starting summary enrichment process")
    def apply_summary(row):
        try:
            ai_summary = summarize(row['content'])
            logging.info(f"Generated AI summary for {row['link']} (first 50 chars): {ai_summary[:50]}...")
            return ai_summary
        except Exception as e:
            logging.error(f"Error summarizing news for {row['link']}: {e}")
            return "Error in summarization"
    
    df['ai_summary'] = df.apply(apply_summary, axis=1)
    logging.info(f"Summary enrichment completed for {len(df)} items")
    return df

def enrich_all(df):
    logging.info("Starting full enrichment process")
    df = enrich_content(df)
    df = enrich_tags(df)
    df = enrich_summary(df)
    logging.info(f"Full enrichment completed. DataFrame now has {len(df.columns)} columns")
    return df
