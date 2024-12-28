import streamlit as st
import uuid
from ai.market_agent import MarketAgent
from utils.db.conversation import store_conversation, get_conversation_history

def initialize_session_state():
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "market_agent" not in st.session_state:
        st.session_state.market_agent = MarketAgent()

def display_chat_history():
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

def get_sample_questions():
    return [
        "How has EUR/USD performed in the last 24 hours?",
        "Show me the USD/JPY trend for the past 7 days",
        "What's the GBP/USD rate for today?",
        "Compare EUR/JPY performance over the last month",
        "What's the current trend for GBP/EUR?",
        "Show me USD/CHF volatility this week"
    ]

st.title("Morpheus - Your Stock Market AI Agent")

initialize_session_state()

# Create two columns - main chat (2/3) and sample questions (1/3)
chat_col, sample_col = st.columns([2, 1])

with chat_col:
    # Add greeting message
    if not st.session_state.messages:
        st.markdown("Hi, I'm Morpheus, your market AI assistant. Ask me anything and magic will happen!")
    
    # Display chat history
    display_chat_history()
    
    # Chat input
    if prompt := st.chat_input("Ask me about currency markets"):
        # Display user message
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Get response from market agent
        response = st.session_state.market_agent.process_query(prompt)
        
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

with sample_col:
    st.markdown("### Sample Questions")
    for question in get_sample_questions():
        if st.button(question, key=f"btn_{hash(question)}"):
            # When a sample question is clicked, send it to the chat
            st.session_state.messages.append({"role": "user", "content": question})
            
            # Get response from market agent
            response = st.session_state.market_agent.process_query(question)
            
            # Add assistant response
            st.session_state.messages.append({"role": "assistant", "content": response})
            
            # Store conversation in database
            store_conversation(
                user_id="anonymous",
                session_id=st.session_state.session_id,
                user_prompt=question,
                answer=response
            )
            
            # Force a rerun to update the chat
            st.rerun()
