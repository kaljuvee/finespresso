import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
from tasks.ai.predict import predict
import re
from utils.enrich_util import determine_event_from_content
from utils.static.tag_util import tag_list
from utils.db.news_db_util import get_news_by_event
from utils.db.price_move_db_util import get_news_price_moves
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

def strip_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def clean_text(text):
    # Remove extra whitespace and newlines
    text = re.sub(r'\s+', ' ', text).strip()
    return text

st.title("News Prediction")

# Text area for pasting news content
news_content = st.text_area("Paste your news content here:", height=200)

# Initialize session state variables
if 'detected_event' not in st.session_state:
    st.session_state.detected_event = None
if 'selected_event' not in st.session_state:
    st.session_state.selected_event = None

# Step 1: Detect Event
if st.button("Detect Event"):
    if news_content:
        # Strip HTML and clean the text
        cleaned_content = clean_text(strip_html(news_content))

        # Determine the event from the content
        st.session_state.detected_event = determine_event_from_content(cleaned_content)

        if st.session_state.detected_event:
            st.write(f"Detected event: {st.session_state.detected_event}")
        else:
            st.error("Unable to determine the event for this news content.")
    else:
        st.warning("Please paste some news content before detecting the event.")

# Step 2: Event Selection Dropdown
if st.session_state.detected_event:
    st.session_state.selected_event = st.selectbox(
        "Select or confirm the event:",
        options=tag_list,
        index=tag_list.index(st.session_state.detected_event) if st.session_state.detected_event in tag_list else 0
    )

# Step 3: Predict
if st.button("Predict"):
    if st.session_state.selected_event:
        # Strip HTML and clean the text (in case it wasn't done before)
        cleaned_content = clean_text(strip_html(news_content))

        # Prepare DataFrame for prediction
        pred_df = pd.DataFrame({'event': [st.session_state.selected_event], 'content': [cleaned_content]})

        # Make predictions
        pred_df = predict(pred_df)

        # Display results
        if 'predicted_move' in pred_df.columns and pd.notna(pred_df['predicted_move'].iloc[0]):
            st.write(f"Predicted move: {pred_df['predicted_move'].iloc[0]:.4f}")
        else:
            st.warning("Unable to predict move for this event.")

        if 'predicted_side' in pred_df.columns and pd.notna(pred_df['predicted_side'].iloc[0]):
            st.write(f"Predicted side: {pred_df['predicted_side'].iloc[0]}")
        else:
            st.warning("Unable to predict side for this event.")

        if 'predicted_move' not in pred_df.columns and 'predicted_side' not in pred_df.columns:
            st.error("No predictions available for this event. Models might be missing.")
    else:
        st.warning("Please detect an event and select it before predicting.")

# New Step: Show Similar News
if st.button("Show Similar"):
    if st.session_state.selected_event:
        # Strip HTML and clean the text (in case it wasn't done before)
        cleaned_content = clean_text(strip_html(news_content))

        # Retrieve similar news and price changes
        news_df = get_news_by_event(st.session_state.selected_event)
        if not news_df.empty:
            # Vectorize and compute cosine similarity
            vectorizer = TfidfVectorizer()
            tfidf_matrix = vectorizer.fit_transform(news_df['content'])
            query_vector = vectorizer.transform([cleaned_content])
            similarity_scores = cosine_similarity(query_vector, tfidf_matrix).flatten()

            # Add similarity scores to DataFrame
            news_df['similarity_score'] = similarity_scores

            # Sort by similarity score and take top 10
            top_news_df = news_df.sort_values(by='similarity_score', ascending=False).head(10)

            # Get price moves
            price_moves_df = get_news_price_moves()

            # Merge dataframes on news_id
            final_df = pd.merge(top_news_df, price_moves_df, left_on='news_id', right_on='id', how='left')

            # Filter out rows where price_change_percentage is NaN
            final_df = final_df[pd.notna(final_df['price_change_percentage'])]

            # Sort by similarity score and take top 10
            final_df = final_df.sort_values(by='similarity_score', ascending=False).head(10)

            # Create clickable links for titles
            final_df['title'] = final_df.apply(lambda row: f'<a href="{row["link"]}" target="_blank">{row["title"]}</a>', axis=1)

            # Display results as a DataFrame with clickable links
            st.write(final_df[['title', 'event', 'similarity_score', 'price_change_percentage']].to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.warning("No similar news found for this event.")
    else:
        st.warning("Please detect an event and select it before showing similar news.")
