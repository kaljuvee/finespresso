import streamlit as st
import pandas as pd
import feedparser
import requests
from dotenv import load_dotenv
from openai import OpenAI  # Ensure this points to your actual OpenAI client and cache module
from gptcache import cache

import os
# Load the environment variables from the .env file
load_dotenv()

#DB_NAME = os.getenv('OPENAI_API_KEY')
#os.environ["OPENAI_API_KEY"] = os.environ.get("OPENAI_API_KEY")

client = OpenAI()
cache.init()
cache.set_openai_key()

def tag_news(news, tags):
    prompt = f'Answering with one tag only, pick up the best tag which describes the news "{news}" from the list: {tags}'
    response = client.chat.completions.create(
        model="gpt-4-0125-preview",
        messages=[{"role": "user", "content": prompt}]
    )
    tag = response.choices[0].message.content
    return tag

def parse_rss_feed(url, tags):
    feed = feedparser.parse(url)
    items = feed.entries

    data = {
        'Title': [],
        'Link': [],
        'Publication Date': [],
        'Issuer': [],
        'Content': [],
        'Event': []  # New column for tagged events
    }

    for item in items:
        title = item.title
        link = item.link
        pub_date = item.published
        issuer = item.get('issuer', 'N/A')
        content = fetch_url_content(link)[:500]  # Limit content to 500 characters for tagging

        # Tag the news content
        event_tag = tag_news(content, tags)

        # Append data
        data['Title'].append(title)
        data['Link'].append(link)
        data['Publication Date'].append(pub_date)
        data['Issuer'].append(issuer)
        data['Content'].append(content)
        data['Event'].append(event_tag)

    return pd.DataFrame(data)

def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return "Failed to fetch content"

def make_clickable(title, link):
    return f'<a target="_blank" href="{link}">{title}</a>'

# Streamlit app title
st.title("NASDAQ Baltic Market News")

# Define your RSS feed URL and tags
rss_url = "https://nasdaqbaltic.com/statistics/en/news?rss=1&num=100&issuer="

tag_list = [
    "shares_issue",
    "observation_status",
    "financial_results",
    "mergers_acquisitions",
    "annual_general_meeting",
    "management_change",
    "annual_report",
    "exchange_announcement",
    "ex_dividend_date",
    "converence_call_webinar",
    "geographic_expansion",
    "analyst_coverage",
    "financial_calendar",
    "share_capital_increase",
    "bond_fixing",
    "fund_data_announcement",
    "capital_investment",
    "calendar_of_events",
    "voting_rights",
    "law_legal_issues",
    "initial_public_offering",
    "regulatory_filings",
    "joint_venture",
    "partnerships",
    "environmental_social_governance",
    "business_contracts",
    "financing_agreements",
    "patents"
]
tags = ",".join(tag_list)

# Parse the RSS feed and tag news
df = parse_rss_feed(rss_url, tags)

# Format 'Title' column to hyperlink
df['Title'] = df.apply(lambda x: make_clickable(x['Title'], x['Link']), axis=1)

# Display DataFrame in Streamlit, including the new 'Event' column
st.write(df[['Title', 'Publication Date', 'Issuer', 'Event']].to_html(escape=False, index=False), unsafe_allow_html=True)