import yfinance as yf
import pandas as pd
from openai import OpenAI
import json
from datetime import datetime, timedelta
import pytz
import numpy as np
from typing import List, Dict, Union, Optional
import requests
import logging
from .base_agent import BaseAgent
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class MarketAgent(BaseAgent):
    def __init__(self):
        # Get API key from environment variable
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        super().__init__(openai_api_key)
        self.model_name = self.default_model  # Use default model from base agent

    def set_model(self, model_name: str):
        """Set the OpenAI model to use"""
        self.model_name = model_name
        # Optionally reset conversation history when model changes
        self.reset_conversation()

    def _initialize(self):
        """Initialize agent-specific components"""
        self.client = OpenAI(api_key=self.openai_api_key)
        self.use_plotly = False  # Default to simple line chart
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_stock_data",
                    "description": "Get historical stock price data and create a chart. Always use this for stock price queries.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "symbol": {
                                "type": "string",
                                "description": "The stock symbol (e.g., AAPL, TSLA) or company name"
                            },
                            "period": {
                                "type": "string",
                                "enum": ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "max"],
                                "description": "The time period for analysis"
                            }
                        },
                        "required": ["symbol", "period"]
                    }
                }
            }
        ]
        
        self.conversation_history = [
            {"role": "system", "content": """You are a financial market analysis assistant. 
            IMPORTANT: For ANY question about stock performance or price movements, you MUST:
            1. ALWAYS call get_stock_data function first
            2. Use the function's response to provide analysis
            3. Never try to answer price-related questions without calling get_stock_data
            
            Time period mapping (use exactly these values):
            - "today" or "24 hours" → "1d"
            - "week" or "5 days" → "5d"
            - "month" or "30 days" → "1mo"
            - "3 months" or "quarter" → "3mo"
            - "6 months" or "half year" → "6mo"
            - "year" or "12 months" → "1y"
            - "2 years" → "2y"
            - "5 years" → "5y"
            
            For stock performance questions:
            1. First identify the company/symbol and time period
            2. Call get_stock_data with these parameters
            3. Wait for the data
            4. Then analyze:
               - Price changes
               - Trends
               - Notable movements
               - Volume patterns if available
            
            Remember: NEVER skip calling get_stock_data for price-related queries."""}
        ]

    def process_question(self, question: str) -> str:
        """Implementation of abstract method from BaseAgent"""
        return self.process_financial_query(question)

    def find_ticker(self, company_query: str) -> Dict:
        """Find the most relevant ticker for a company query"""
        try:
            url = "https://query2.finance.yahoo.com/v1/finance/search"
            params = {
                'q': company_query,
                'quotesCount': 5,
                'newsCount': 0,
                'enableFuzzyQuery': True,
                'quotesQueryId': 'tss_match_phrase_query'
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if not data.get("quotes"):
                return {"error": f"No ticker found for {company_query}"}
                
            # Filter for stocks and sort by score
            stocks = [q for q in data["quotes"] if q.get("quoteType") == "EQUITY"]
            if not stocks:
                return {"error": f"No stock ticker found for {company_query}"}
                
            best_match = stocks[0]
            return {
                "symbol": best_match["symbol"],
                "name": best_match.get("shortname", best_match.get("longname")),
                "exchange": best_match.get("exchange"),
                "score": best_match.get("score")
            }
            
        except Exception as e:
            return {"error": f"Error finding ticker: {str(e)}"}

    def get_stock_data(self, symbol: str, period: str = "1y") -> Dict:
        """Fetch stock data and return formatted information"""
        try:
            # First verify/lookup the ticker if it might be a company name
            if not symbol.isupper() or len(symbol) > 5:
                ticker_info = self.find_ticker(symbol)
                if "error" in ticker_info:
                    return ticker_info
                symbol = ticker_info["symbol"]

            stock = yf.Ticker(symbol)
            df = stock.history(period=period)
            
            if df.empty:
                return {"error": f"No data available for {symbol}"}
            
            # Calculate price change and percentage
            first_close = df['Close'].iloc[0]
            last_close = df['Close'].iloc[-1]
            price_change = last_close - first_close
            price_change_pct = (price_change / first_close) * 100
            
            data_dict = {
                "timestamps": [int(ts.timestamp() * 1000) for ts in df.index],
                "prices": df['Close'].tolist(),
            }
            if 'Volume' in df.columns:
                data_dict["volume"] = df['Volume'].tolist()
            
            return {
                "symbol": symbol,
                "data": data_dict,
                "summary": {
                    "first_close": first_close,
                    "last_close": last_close,
                    "price_change": price_change,
                    "price_change_pct": price_change_pct,
                    "start_time": df.index[0].strftime('%Y-%m-%d'),
                    "end_time": df.index[-1].strftime('%Y-%m-%d'),
                    "period": period
                },
                "display_graph": True
            }
        except Exception as e:
            return {"error": f"Error fetching data for {symbol}: {str(e)}"}

    def get_stock_price(self, company: str) -> Dict:
        """Get current stock price and basic information"""
        try:
            # First verify/lookup the ticker if it might be a company name
            if not company.isupper() or len(company) > 5:
                ticker_info = self.find_ticker(company)
                if "error" in ticker_info:
                    return ticker_info
                company = ticker_info["symbol"]

            stock = yf.Ticker(company)
            info = stock.info
            
            return {
                "symbol": company,
                "name": info.get('longName', company),
                "current_price": info.get('currentPrice', 'N/A'),
                "previous_close": info.get('previousClose', 'N/A'),
                "market_cap": info.get('marketCap', 'N/A'),
                "volume": info.get('volume', 'N/A'),
                "exchange": info.get('exchange', 'N/A')
            }
        except Exception as e:
            return {"error": f"Error getting stock price for {company}: {str(e)}"}

    def get_stock_news(self, company: str) -> Dict:
        """Get recent news articles about a company"""
        try:
            # First verify/lookup the ticker if it might be a company name
            if not company.isupper() or len(company) > 5:
                ticker_info = self.find_ticker(company)
                if "error" in ticker_info:
                    return ticker_info
                company = ticker_info["symbol"]

            stock = yf.Ticker(company)
            news = stock.news[:5]
            
            news_data = []
            for item in news:
                news_data.append({
                    "title": item.get('title', ''),
                    "publisher": item.get('publisher', ''),
                    "link": item.get('link', ''),
                    "published": datetime.fromtimestamp(item.get('providerPublishTime', 0)).strftime('%Y-%m-%d %H:%M:%S')
                })
            
            return {
                "symbol": company,
                "news": news_data
            }
        except Exception as e:
            return {"error": f"Error getting news for {company}: {str(e)}"}

    def toggle_plotly(self, use_plotly: bool):
        """Toggle between Plotly and simple line charts"""
        self.use_plotly = use_plotly

    def create_plotly_chart(self, data: Dict) -> Dict:
        """Create a Plotly chart from the stock data"""
        try:
            timestamps = [datetime.fromtimestamp(ts/1000) for ts in data["data"]["timestamps"]]
            prices = data["data"]["prices"]
            symbol = data["symbol"]
            
            # Create figure with secondary y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add candlestick chart
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
            if "volume" in data["data"]:
                fig.add_trace(
                    go.Bar(
                        x=timestamps,
                        y=data["data"]["volume"],
                        name="Volume",
                        marker=dict(color='#B2DFDB'),
                        opacity=0.5
                    ),
                    secondary_y=True
                )
            
            # Calculate price change for title
            price_change = data["summary"]["price_change"]
            price_change_pct = data["summary"]["price_change_pct"]
            change_color = "green" if price_change >= 0 else "red"
            
            # Update layout with more details
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
                margin=dict(t=100)  # Increase top margin for title
            )
            
            # Update y-axes ranges
            fig.update_yaxes(title_text="Price ($)", secondary_y=False)
            if "volume" in data["data"]:
                fig.update_yaxes(title_text="Volume", secondary_y=True)
            
            return {
                "plotly_chart": fig.to_dict(),
                "symbol": symbol,
                "period": data["summary"]["period"]
            }
            
        except Exception as e:
            logging.error(f"Error creating Plotly chart: {str(e)}")
            return {"error": f"Error creating Plotly chart: {str(e)}"}

    def format_response(self, message: str, data: Optional[Dict] = None) -> Dict:
        """Format the response with optional graph data"""
        response = {
            "response": message.strip(),
            "success": True
        }
        
        if data:
            # Always include graph data if present
            response["graph_data"] = {
                "symbol": data["symbol"],
                "data": data["data"],
                "summary": data["summary"]
            }
            
            # Add Plotly data if enabled
            if self.use_plotly and data.get("display_graph"):
                plotly_data = self.create_plotly_chart(data)
                if "error" not in plotly_data:
                    response["plotly_data"] = plotly_data
        
        return response

    def process_financial_query(self, query: str) -> str:
        """Process market analysis queries using function calling"""
        try:
            self.conversation_history.append({"role": "user", "content": query})
            
            # First API call to get tool calls
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=self.conversation_history,
                tools=self.tools,
                tool_choice={"type": "function", "function": {"name": "get_stock_data"}}  # Force function call
            )
            
            message = response.choices[0].message
            
            # Add assistant's message to history
            self.conversation_history.append({
                "role": "assistant",
                "content": message.content if message.content else "",
                "tool_calls": message.tool_calls
            })
            
            # Handle tool calls
            if message.tool_calls:
                function_results = []
                
                for tool_call in message.tool_calls:
                    function_args = json.loads(tool_call.function.arguments)
                    
                    # Get stock data
                    result = self.get_stock_data(
                        function_args["symbol"],
                        function_args.get("period", "1y")
                    )
                    
                    if result and "error" not in result:
                        function_results.append(result)
                        self.conversation_history.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "name": "get_stock_data",
                            "content": json.dumps(result, cls=DateTimeEncoder)
                        })
                
                # Get final response with the data
                final_response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=self.conversation_history
                )
                
                final_message = final_response.choices[0].message
                self.conversation_history.append({
                    "role": "assistant",
                    "content": final_message.content
                })
                
                # Return response with graph data
                if function_results:
                    return json.dumps(
                        self.format_response(final_message.content, function_results[0]),
                        cls=DateTimeEncoder
                    )
            
            # If somehow we got here without data, force a stock data request
            if "stock" in query.lower() or "price" in query.lower():
                # Extract company name/symbol (simple approach)
                words = query.split()
                for word in words:
                    if word.isupper():
                        result = self.get_stock_data(word, "5d")
                        if "error" not in result:
                            return json.dumps(
                                self.format_response(f"Here's the stock data for {word}", result),
                                cls=DateTimeEncoder
                            )
            
            return json.dumps(
                self.format_response(message.content),
                cls=DateTimeEncoder
            )
            
        except Exception as e:
            logging.error(f"Error processing query: {str(e)}")
            return json.dumps({
                "response": f"I encountered an error while processing your request: {str(e)}",
                "success": False
            }, cls=DateTimeEncoder)

    def reset_conversation(self):
        """Reset the conversation history"""
        self.conversation_history = [self.conversation_history[0]]  # Keep system message
