import streamlit as st
import yfinance as yf
from datetime import datetime, timedelta
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain.chat_models import ChatOpenAI
from langchain.agents.agent_types import AgentType
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

st.title("Stock Price Data")

# Check for OpenAI API key
if not os.getenv("OPENAI_API_KEY"):
    st.warning("Please add your OpenAI API key to continue.")
    st.stop()

# Sidebar inputs
with st.sidebar:
    # Ticker input
    ticker = st.text_input("Enter Stock Ticker", value="AAPL").upper()
    
    # Interval selection
    interval = st.selectbox(
        "Select Time Interval",
        options=['5m', '15m', '30m', '60m'],
        index=0  # Default to 5m
    )
    
    # Date range selection with default 7 days back
    end_date = datetime.now().date()
    default_start = end_date - timedelta(days=7)
    
    date_range = st.date_input(
        "Select Date Range",
        value=(default_start, end_date),
        max_value=end_date,
        help="Select your date range"
    )
    
    # Add Get Data button
    get_data = st.button("Get Data", type="primary")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "df" not in st.session_state:
    st.session_state.df = None
if "current_ticker" not in st.session_state:
    st.session_state.current_ticker = None

# Main content
if get_data and ticker != st.session_state.current_ticker:
    try:
        start_date, end_date = date_range
        # Download data
        df = yf.download(ticker, interval=interval, start=start_date, end=end_date)
        
        if not df.empty:
            # Update session state
            st.session_state.df = df
            st.session_state.current_ticker = ticker
            
            # Create new DataFrame agent for new data
            st.session_state.agent = create_pandas_dataframe_agent(
                ChatOpenAI(
                    temperature=0,
                    model="gpt-4o-mini",
                    api_key=os.getenv("OPENAI_API_KEY")
                ),
                df,
                verbose=True,
                agent_type=AgentType.OPENAI_FUNCTIONS,
                allow_dangerous_code=True
            )
        else:
            st.warning(f"No data available for {ticker}")
            
    except Exception as e:
        st.error(f"Error occurred: {str(e)}")

# Display data and chat interface if we have data
if st.session_state.df is not None:
    # Display dataframe with filtering
    st.write(f"### {st.session_state.current_ticker} Price Data ({interval})")
    st.dataframe(
        st.session_state.df,
        column_config={
            "Open": st.column_config.NumberColumn(format="%.2f"),
            "High": st.column_config.NumberColumn(format="%.2f"),
            "Low": st.column_config.NumberColumn(format="%.2f"),
            "Close": st.column_config.NumberColumn(format="%.2f"),
            "Volume": st.column_config.NumberColumn(format="%d")
        },
        hide_index=False,
        use_container_width=True,
        height=400
    )

    # Chat interface
    st.write("### Chat with your data")
    st.info("""Try asking questions like:
    - What's the highest price in this period?
    - What's the average volume?
    - What day had the biggest price drop?
    - Show me the trend analysis
    """)

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask about your data"):
        # Add user message to chat message container
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Add assistant response to chat message container
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    response = st.session_state.agent.run(prompt)
                    st.markdown(response)
                    # Only append messages if we successfully got a response
                    st.session_state.messages.append({"role": "user", "content": prompt})
                    st.session_state.messages.append({"role": "assistant", "content": response})
                except Exception as e:
                    error_msg = f"Error generating response: {str(e)}"
                    st.error(error_msg)
else:
    st.info("👈 Enter a ticker symbol and click 'Get Data' to view the data")
