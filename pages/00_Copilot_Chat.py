import streamlit as st
import uuid
from ai.routing_agent import RoutingAgent
from utils.db.conversation import store_conversation, get_conversation_history
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger(__name__)

def initialize_session_state():
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "routing_agent" not in st.session_state:
        st.session_state.routing_agent = RoutingAgent()
    if "use_plotly" not in st.session_state:
        st.session_state.use_plotly = False
    if "model_name" not in st.session_state:
        st.session_state.model_name = "gpt-4-turbo-preview"

def display_chat_history():
    """Display the chat history in the Streamlit interface"""
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

def get_sample_questions():
    return {
        "Stock Prices": [
            "market: What's the current stock price of Apple?",
            "market: How has Tesla's stock performed over the last 5 days?",
            "market: Compare Microsoft and Google stock prices for the past month",
            "market: What was Amazon's closing price yesterday?"
        ],
        
        "Market Cap and Fundamentals": [
            "market: What is NVIDIA's current market capitalization?",
            "market: Show me Meta's P/E ratio and market fundamentals",
            "market: What are Apple's key financial metrics?",
            "market: Compare the market cap of Tesla and Toyota"
        ],
        
        "Market News and Analysis": [
            "market: What are the latest news headlines for Netflix?",
            "market: Tell me about recent developments at Microsoft",
            "market: What's happening with AMD stock lately?",
            "market: Show me news that might affect Amazon's stock price"
        ],
        
        "Technical Analysis": [
            "market: Show me a price chart for Bitcoin over the last 3 months",
            "market: What's the trading volume for GameStop today?",
            "market: Analyze the trend for Disney stock this year",
            "market: Show me the volatility of S&P 500 this month"
        ],
        
        "Mixed Market Queries": [
            "market: Give me a complete analysis of Apple including price, news, and fundamentals",
            "market: What's the outlook for Tesla based on recent news and price action?",
            "market: Compare Netflix and Disney's performance and latest developments",
            "market: Analyze Intel's stock movement and recent announcements"
        ],
        
        "News & Predictions": [
            "news: What are the latest merger-related predictions?",
            "news: Show me today's top 5 predicted stock moves",
            "news: Which companies have the highest predicted moves this week?",
            "news: List all acquisition predictions from last 3 days",
            "news: What are the trending market events today?",
            "news: Show me predictions for companies in the tech sector"
        ]
    }

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
        st.session_state.routing_agent.set_model(new_model)
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
        st.session_state.routing_agent.toggle_plotly(use_plotly)
    
    # Add session management section
    st.markdown("### Session Management")
    if st.button("🔄 Refresh Session", help="Clear current session and start fresh"):
        # Clear all session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        # Rerun the app
        st.rerun()
        # Add disclaimer at the top of sidebar
    st.markdown("""
    ### Finespresso Copilot
    
    **Disclaimer:** Finespresso Copilot is an experiment brought to you by Finespresso LLC. Finespresso Copilot is an AI research tool powered by a generative large language model. Finespresso Copilot is experimental technology and may give inaccurate or inappropriate responses. Output from Finespresso Copilot should not be construed as investment research or recommendations, and should not serve as the basis for any investment decision. All Finespresso Copilot output is provided "as is." Finespresso makes no representations or warranties with respect to the accuracy, completeness, quality, timeliness, or any other characteristic of such output. Your use of Finespresso Copilot output is at your sole risk. Please independently evaluate and verify the accuracy of any such output for your own use case.
    
    ---
    """)
    
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
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        try:
            # Use routing agent to handle query
            response_data = st.session_state.routing_agent.route_query(prompt)
            
            if not response_data.get("success", True):
                st.error(response_data["response"])
                response_for_history = response_data["response"]
            else:
                st.chat_message("assistant").markdown(response_data["response"])
                
                # Handle graph display if present
                if "graph_data" in response_data:
                    try:
                        if st.session_state.use_plotly:
                            fig = create_stock_chart(response_data["graph_data"], use_plotly=True)
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            chart_data = create_stock_chart(response_data["graph_data"], use_plotly=False)
                            st.line_chart(chart_data['price'])
                    except Exception as e:
                        st.warning(f"Could not display chart: {str(e)}")
                        logger.error(f"Chart display error: {str(e)}")
                
                response_for_history = response_data["response"]
            
            # Add response to chat history and store conversation
            st.session_state.messages.append({
                "role": "assistant",
                "content": response_for_history
            })
            
            store_conversation(
                user_id="anonymous",
                session_id=st.session_state.session_id,
                user_prompt=prompt,
                answer=response_for_history
            )
            
        except Exception as e:
            st.error(f"An unexpected error occurred: {str(e)}")

with sample_col:
    st.markdown("### Sample Questions")
    # Get questions dictionary
    categories = get_sample_questions()
    
    # Display each category and its questions
    for category, questions in categories.items():
        st.markdown(f"#### {category}")
        for question in questions:
            if st.button(question, key=f"btn_{hash(question)}"):
                st.session_state.messages.append({"role": "user", "content": question})
                
                try:
                    # Check if it's a news query
                    if question.lower().startswith("news:"):
                        # Use DB Agent for news queries
                        logger.info(f"Processing news query: {question}")  # Add logging
                        query, results = st.session_state.db_agent.process_question(question[5:].strip())
                        logger.info(f"DB Query executed: {query}")  # Add logging
                        
                        # Format the response
                        if results is not None and not results.empty:
                            logger.info(f"Query returned {len(results)} rows")  # Add logging
                            response_data = {
                                "response": f"Here are the results:\n\n{results.to_markdown()}",
                                "success": True
                            }
                        else:
                            logger.warning("Query returned no results")  # Add logging
                            response_data = {
                                "response": "No results found for your query.",
                                "success": True
                            }
                    else:
                        # Use Market Agent for other queries
                        response = st.session_state.market_agent.process_financial_query(question)
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
                    
                except Exception as e:
                    st.error(f"Error processing response: {str(e)}")
                
                st.rerun()
