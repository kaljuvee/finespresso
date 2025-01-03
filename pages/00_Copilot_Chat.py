import streamlit as st
import uuid
from ai.market_agent import MarketAgent
from utils.db.conversation import store_conversation, get_conversation_history
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import logging

def initialize_session_state():
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "market_agent" not in st.session_state:
        st.session_state.market_agent = MarketAgent()
    if "use_plotly" not in st.session_state:
        st.session_state.use_plotly = False
    if "model_name" not in st.session_state:
        st.session_state.model_name = "gpt-4-turbo-preview"  # Default model

def display_chat_history():
    """Display the chat history in the Streamlit interface"""
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

def get_sample_questions():
    return [
        # Stock Price Questions
        "What's the current stock price of Apple?",
        "How has Tesla's stock performed over the last 5 days?",
        "Compare Microsoft and Google stock prices for the past month",
        "What was Amazon's closing price yesterday?",
        
        # Market Cap and Fundamentals
        "What is NVIDIA's current market capitalization?",
        "Show me Meta's P/E ratio and market fundamentals",
        "What are Apple's key financial metrics?",
        "Compare the market cap of Tesla and Toyota",
        
        # News and Analysis
        "What are the latest news headlines for Netflix?",
        "Tell me about recent developments at Microsoft",
        "What's happening with AMD stock lately?",
        "Show me news that might affect Amazon's stock price",
        
        # Technical Analysis
        "Show me a price chart for Bitcoin over the last 3 months",
        "What's the trading volume for GameStop today?",
        "Analyze the trend for Disney stock this year",
        "Show me the volatility of S&P 500 this month",
        
        # Mixed Queries
        "Give me a complete analysis of Apple including price, news, and fundamentals",
        "What's the outlook for Tesla based on recent news and price action?",
        "Compare Netflix and Disney's performance and latest developments",
        "Analyze Intel's stock movement and recent announcements"
    ]

def create_stock_chart(graph_data: dict, use_plotly: bool = False):
    """Create either a Plotly or simple line chart from graph data"""
    try:
        # Convert timestamps to datetime
        timestamps = [datetime.fromtimestamp(ts/1000) for ts in graph_data["data"]["timestamps"]]
        prices = graph_data["data"]["prices"]
        symbol = graph_data["symbol"]
        
        if use_plotly:
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add price line
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=prices,
                    name=f"{symbol} Price",
                    line=dict(color='#2962FF', width=2),
                    showlegend=True
                ),
                secondary_y=False
            )
            
            # Add volume bars if available
            if "volume" in graph_data["data"]:
                fig.add_trace(
                    go.Bar(
                        x=timestamps,
                        y=graph_data["data"]["volume"],
                        name="Volume",
                        marker=dict(color='#B2DFDB'),
                        opacity=0.5
                    ),
                    secondary_y=True
                )
            
            # Calculate price change for title
            price_change = graph_data["summary"]["price_change"]
            price_change_pct = graph_data["summary"]["price_change_pct"]
            change_color = "green" if price_change >= 0 else "red"
            
            # Update layout
            fig.update_layout(
                title=dict(
                    text=f"{symbol} Stock Price<br>"
                         f"<span style='color: {change_color}'>Change: "
                         f"${price_change:.2f} ({price_change_pct:.2f}%)</span>",
                    x=0.5,
                    xanchor='center'
                ),
                xaxis=dict(
                    title="Date",
                    rangeslider=dict(visible=False)
                ),
                yaxis=dict(
                    title="Price ($)",
                    tickformat=".2f"
                ),
                yaxis2=dict(
                    title="Volume",
                    showgrid=False
                ),
                template="plotly_white",
                hovermode='x unified',
                height=500,
                margin=dict(t=100)
            )
            
            return fig
        else:
            # Create simple line chart data
            chart_data = pd.DataFrame({
                'timestamp': timestamps,
                'price': prices
            })
            return chart_data.set_index('timestamp')
            
    except Exception as e:
        logging.error(f"Error creating chart: {str(e)}")
        raise e

# Initialize session state first
initialize_session_state()

st.title("Finespresso Copilot")

# Move sidebar configuration to top of the page
with st.sidebar:
    st.markdown("### Settings")
    
    # Model selection
    model_options = {
        "GPT-4o-mini": "gpt-4o-mini",
        "GPT-4o": "gpt-4o",
    }
    selected_model = st.selectbox(
        "Select Model",
        options=list(model_options.keys()),
        index=0,
        help="Choose between different GPT models"
    )
    
    # Update model if changed
    new_model = model_options[selected_model]
    if new_model != st.session_state.model_name:
        st.session_state.model_name = new_model
        st.session_state.market_agent.set_model(new_model)
        # Optional: Clear chat history when model changes
        st.session_state.messages = []
    
    # Visualization toggle
    st.markdown("### Visualization")
    use_plotly = st.checkbox(
        "Use Plotly for graphs",
        value=st.session_state.use_plotly,
        help="Toggle between simple charts and interactive Plotly graphs"
    )
    if use_plotly != st.session_state.use_plotly:
        st.session_state.use_plotly = use_plotly
        st.session_state.market_agent.toggle_plotly(use_plotly)
    
    # Add session management section
    st.markdown("### Session Management")
    if st.button("🔄 Refresh Session", help="Clear current session and start fresh"):
        # Clear all session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        # Rerun the app
        st.rerun()

# Create two columns - main chat (2/3) and sample questions (1/3)
chat_col, sample_col = st.columns([2, 1])

with chat_col:
    # Add greeting message
    if not st.session_state.messages:
        st.markdown("""
        Hi, I'm your GenAI Financial Markets Assistant! I can help you with:
        - Real-time stock prices and market data
        - Company news and developments
        - Market analysis and trends
        - Financial metrics and fundamentals
        
        Ask me anything about stocks, companies, or markets!
        """)
    
    # Display chat history
    display_chat_history()
    
    # Chat input
    if prompt := st.chat_input("Ask me about stocks, markets, or companies..."):
        # Display user message
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            # Get response from market agent
            response = st.session_state.market_agent.process_financial_query(prompt)
            response_data = json.loads(response)
            
            if not response_data.get("success", True):
                # Handle error response
                st.error(response_data["response"])
                response_for_history = response_data["response"]
            else:
                # Display the text response
                st.chat_message("assistant").markdown(response_data["response"])
                
                # Handle graph display
                if "graph_data" in response_data:
                    try:
                        if st.session_state.use_plotly:
                            # Create and display Plotly chart
                            fig = create_stock_chart(response_data["graph_data"], use_plotly=True)
                            st.plotly_chart(
                                fig,
                                use_container_width=True,
                                config={
                                    'displayModeBar': True,
                                    'scrollZoom': True
                                }
                            )
                        else:
                            # Create and display simple line chart
                            chart_data = create_stock_chart(response_data["graph_data"], use_plotly=False)
                            st.line_chart(chart_data['price'])
                    except Exception as e:
                        st.warning(f"Could not display chart: {str(e)}")
                        logging.error(f"Chart display error: {str(e)}")
                
                response_for_history = response_data["response"]
            
            # Add response to chat history
            st.session_state.messages.append({
                "role": "assistant",
                "content": response_for_history
            })
            
            # Store conversation in database
            store_conversation(
                user_id="anonymous",
                session_id=st.session_state.session_id,
                user_prompt=prompt,
                answer=response_for_history
            )
            
        except json.JSONDecodeError as e:
            st.error(f"Error processing response: {str(e)}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")

with sample_col:
    st.markdown("### Sample Questions")
    # Group questions by category
    categories = {
        "Stock Prices": get_sample_questions()[:4],
        "Fundamentals": get_sample_questions()[4:8],
        "News & Analysis": get_sample_questions()[8:12],
        "Technical Analysis": get_sample_questions()[12:16],
        "Complete Analysis": get_sample_questions()[16:]
    }
    
    for category, questions in categories.items():
        st.markdown(f"#### {category}")
        for question in questions:
            if st.button(question, key=f"btn_{hash(question)}"):
                st.session_state.messages.append({"role": "user", "content": question})
                response = st.session_state.market_agent.process_financial_query(question)
                try:
                    response_data = json.loads(response)
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": response_data["response"]
                    })
                    store_conversation(
                        user_id="anonymous",
                        session_id=st.session_state.session_id,
                        user_prompt=question,
                        answer=response_data["response"]
                    )
                except json.JSONDecodeError:
                    st.error("Error processing response")
                st.rerun()
