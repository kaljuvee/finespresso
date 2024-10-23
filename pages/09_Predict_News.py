import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
from tasks.ai.predict import predict
import re
from utils.enrich_util import determine_event_from_content
from utils.static.tag_util import tag_list

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
