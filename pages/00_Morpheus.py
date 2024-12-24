import streamlit as st
import uuid
from ai.market_agent import MarketAgent
from utils.db.conversation import store_conversation, get_conversation_history

def initialize_session_state():
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if "messages" not in st.session_state:
        st.session_state.messages = []

def display_chat_history():
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

def main():
    st.title("Morpheus - Your Stock Market AI Agent")
    
    initialize_session_state()
    
    # Add greeting message
    if not st.session_state.messages:
        st.markdown("Hi, I'm Morpheus, your market AI assistant. Ask me anything and magic will happen!")
    
    # Initialize market agent
    market_agent = MarketAgent()
    
    # Display chat history
    display_chat_history()
    
    # Chat input
    if prompt := st.chat_input("Ask me about currency markets"):
        # Display user message
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Get response from market agent
        response = market_agent.process_query(prompt)
        
        # Display assistant response
        st.chat_message("assistant").markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})
        
        # Store conversation in database
        store_conversation(
            user_id="anonymous",  # You can implement user authentication later
            session_id=st.session_state.session_id,
            user_prompt=prompt,
            answer=response
        )

if __name__ == "__main__":
    main()
