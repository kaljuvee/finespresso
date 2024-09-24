import streamlit as st
import requests
import tempfile
import os
from dotenv import load_dotenv

load_dotenv()

ELEVENLABS_API_KEY = os.getenv('ELEVENLABS_API_KEY')
# ElevenLabs API settings
ELEVENLABS_VOICE_ID = "Xb7hH8MSUJpSbSDYk0k2"
ELEVENLABS_URL = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}"

def text_to_speech(text, language):
    headers = {
        "Accept": "audio/mpeg",
        "Content-Type": "application/json",
        "xi-api-key": ELEVENLABS_API_KEY
    }
    
    data = {
        "text": text,
        "model_id": "eleven_monolingual_v1",
        "voice_settings": {
            "stability": 0.5,
            "similarity_boost": 0.5
        },
        "language": language  # Add language to the request data
    }
    
    response = requests.post(ELEVENLABS_URL, json=data, headers=headers)
    
    if response.status_code == 200:
        return response.content
    else:
        st.error(f"Error: {response.status_code} - {response.text}")
        return None

def main():
    st.title("ElevenLabs Text-to-Speech with Streamlit")
    
    text_input = st.text_area("Enter the text you want to convert to speech:", "Hello, this is a test of ElevenLabs text-to-speech with Streamlit.")
    
    language = st.selectbox("Choose a language:", ["en", "fr", "de", "it", "es", "sv"])
    
    if st.button("Generate Speech"):
        audio_content = text_to_speech(text_input, language)
        
        if audio_content:
            # Create a temporary file to store the audio
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp_file:
                tmp_file.write(audio_content)
                tmp_file_path = tmp_file.name
            
            # Display audio player
            st.audio(tmp_file_path, format="audio/mp3")
            
            # Provide download button
            with open(tmp_file_path, "rb") as file:
                btn = st.download_button(
                    label="Download audio",
                    data=file,
                    file_name="elevenlabs_audio.mp3",
                    mime="audio/mp3"
                )
            
            # Clean up the temporary file
            os.unlink(tmp_file_path)

if __name__ == "__main__":
    main()
