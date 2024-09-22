import streamlit as st
import pandas as pd
import feedparser
import requests
from bs4 import BeautifulSoup
from gptcache import cache
import os
from datetime import datetime, timedelta
from utils.openai_utils import summarize, tag_news


@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # Extract text from paragraphs, removing any scripts or styles
        for script in soup(["script", "style"]):
            script.decompose()
        text = ' '.join([p.get_text() for p in soup.find_all('p')])
        return text[:1000]  # Return first 1000 characters
    except requests.RequestException:
        return "Failed to fetch content"

@st.cache_data(ttl=3600)  # Cache for 1 hour
def parse_rss_feed(url, tags):
    feed = feedparser.parse(url)
    items = feed.entries[:100]  # Limit to 100 news items

    data = []

    for item in items:
        title = item.title
        link = item.link
        pub_date = item.published
        issuer = item.get('issuer', 'N/A')
        content = fetch_url_content(link)

        try:
            event_tag = tag_news(content, tags)
        except Exception as e:
            st.warning(f"Error tagging news: {str(e)}")
            event_tag = "Error in tagging"

        try:
            exciting_summary = summarize(content)
        except Exception as e:
            st.warning(f"Error summarizing news: {str(e)}")
            exciting_summary = "Error in summarization"

        data.append({
            'Title': title,
            'Link': link,
            'Publication Date': pub_date,
            'Issuer': issuer,
            'Content': content,
            'Event': event_tag,
            'Why it Moves?': exciting_summary
        })

    df = pd.DataFrame(data)
    
    # Ensure all expected columns are present
    for col in ['Title', 'Publication Date', 'Issuer', 'Event', 'Why it Moves?']:
        if col not in df.columns:
            df[col] = "N/A"

    return df

def make_clickable(title, link):
    return f'<a target="_blank" href="{link}">{title}</a>'

def create_audio_player(index):
    audio_file = f"media/voice_{index}.mp3"
    return f'<audio controls><source src="{audio_file}" type="audio/mpeg">Your browser does not support the audio element.</audio>'

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

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.experimental_rerun()

# Parse the RSS feed and tag news
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_cached_dataframe():
    df = parse_rss_feed(rss_url, tags)
    df['Title'] = df.apply(lambda x: make_clickable(x['Title'], x['Link']), axis=1)
    df['Podcast'] = df.index.map(create_audio_player)
    return df

df = get_cached_dataframe()

# Pagination
items_per_page = 25
total_pages = len(df) // items_per_page + (1 if len(df) % items_per_page > 0 else 0)

# Add a select box for page selection
page = st.selectbox("Select Page", options=range(1, total_pages + 1))

# Calculate start and end indices for the current page
start_idx = (page - 1) * items_per_page
end_idx = start_idx + items_per_page

# Display DataFrame for the current page
columns_to_display = ['Title', 'Publication Date', 'Issuer', 'Event', 'Why it Moves?', 'Podcast']
st.write(df[columns_to_display][start_idx:end_idx].to_html(escape=False, index=False), unsafe_allow_html=True)

# Display pagination information
st.write(f"Page {page} of {total_pages}")

# Display last update time
st.write(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
