from langchain.chat_models import ChatOpenAI
from langchain.prompts import SystemMessagePromptTemplate, HumanMessagePromptTemplate, ChatPromptTemplate
from langchain.schema import SystemMessage, HumanMessage
from langchain_experimental.agents import create_pandas_dataframe_agent
import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import re
import json

class MarketAgent:
    def __init__(self):
        self.llm = ChatOpenAI(
            model_name="gpt-4",
            temperature=0.7,
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        with open("prompts/market_agent.md", "r") as f:
            self.system_template = f.read()
    
    def parse_timeframe(self, text):
        """Use LLM to parse timeframe from user input"""
        timeframe_prompt = """Parse the timeframe from this query: "{text}"
        Return only the JSON response in the specified format."""
        
        messages = [
            SystemMessage(content=self.system_template),
            HumanMessage(content=timeframe_prompt.format(text=text))
        ]
        
        try:
            response = self.llm(messages)
            timeframe_data = json.loads(response.content)
            
            # Convert parsed timeframe to timedelta
            unit = timeframe_data['unit']
            amount = timeframe_data['amount']
            
            if unit == 'hours':
                return timedelta(hours=amount)
            elif unit == 'days':
                return timedelta(days=amount)
            elif unit == 'weeks':
                return timedelta(days=amount * 7)
            elif unit == 'months':
                return timedelta(days=amount * 30)
            else:
                return timedelta(hours=24)  # Default fallback
                
        except Exception as e:
            print(f"Error parsing timeframe: {e}")
            return timedelta(hours=24)  # Default fallback
            
    def get_currency_data(self, currency_pair, user_input):
        # Get timeframe using LLM
        timeframe = self.parse_timeframe(user_input)
        
        # Format currency pair exactly like rate_util.py does
        ticker_symbol = currency_pair.replace('/', '') + '=X'
        print(f"Debug - Initial ticker symbol: {ticker_symbol}")  # Debug log
        
        ticker = yf.Ticker(ticker_symbol)
        end_date = datetime.now()
        start_date = end_date - timeframe
        
        # Adjust interval based on timeframe
        if timeframe <= timedelta(days=1):
            interval = "1h"  # Use 1h instead of 15m for more reliable data
        elif timeframe <= timedelta(days=7):
            interval = "1h"
        else:
            interval = "1d"
        
        print(f"Debug - Fetching data for: {ticker_symbol} with interval {interval}")  # Debug log
        data = ticker.history(start=start_date, end=end_date, interval=interval)
        
        # If data is empty, try the inverse pair
        if data.empty:
            # Swap the currencies and try again
            base, quote = currency_pair.split('/')
            inverse_pair = f"{quote}/{base}"
            ticker_symbol = inverse_pair.replace('/', '') + '=X'
            print(f"Debug - Inverse ticker symbol: {ticker_symbol} with interval {interval}")  # Debug log
            
            ticker = yf.Ticker(ticker_symbol)
            data = ticker.history(start=start_date, end=end_date, interval=interval)
            
            # If we got data from inverse pair, invert the rates
            if not data.empty:
                for col in ['Open', 'High', 'Low', 'Close']:
                    data[col] = 1 / data[col]
        
        return data
        
    def perform_analysis(self, df, question):
        """Use pandas DataFrame agent to analyze the rate data"""
        if df.empty:
            return "No data available for analysis."
            
        # Create pandas DataFrame agent
        agent = create_pandas_dataframe_agent(
            self.llm,
            df,
            verbose=True,
            agent_type="openai-tools",
        )
        
        # Prepare analysis prompt
        analysis_prompt = f"""
        Analyze the currency rate data with the following context:
        - First rate: {df['Close'].iloc[0]:.4f}
        - Last rate: {df['Close'].iloc[-1]:.4f}
        - Timeframe: {df.index[0]} to {df.index[-1]}
        
        Question: {question}
        
        Please provide:
        1. The direction of movement
        2. The magnitude of change (absolute and percentage)
        3. Any notable patterns or volatility

        Note: When referring to currency pairs, use format like USDJPY=X (no special characters)
        """
        
        try:
            response = agent.run(analysis_prompt)
            return response
        except Exception as e:
            return f"Error performing analysis: {str(e)}"
        
    def normalize_currency_pair(self, text):
        # Common currency codes
        currencies = ['EUR', 'USD', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'NZD']
        
        # Extract mentioned currencies
        mentioned_currencies = [curr for curr in currencies if curr in text.upper()]
        
        if not mentioned_currencies:
            return None
        elif len(mentioned_currencies) == 1:
            # If only one currency is mentioned, pair it with USD
            base_curr = mentioned_currencies[0]
            quote_curr = 'USD' if base_curr != 'USD' else 'EUR'
            return f"{base_curr}/{quote_curr}"
        else:
            # Use the first two mentioned currencies
            return f"{mentioned_currencies[0]}/{mentioned_currencies[1]}"

    def process_query(self, user_input):
        currency_pair = self.normalize_currency_pair(user_input)
        if not currency_pair:
            return "I couldn't identify any currency pairs in your question. Please mention specific currencies."
            
        # Extract timeframe from user input
        timeframe = self.parse_timeframe(user_input)
        
        # Get historical data
        rate_data = self.get_currency_data(currency_pair, user_input)
        if rate_data.empty:
            return "I couldn't fetch the rate data for the specified timeframe."
            
        # Perform analysis using DataFrame agent
        analysis = self.perform_analysis(rate_data, user_input)
        
        # Prepare final response using system template
        system_message = SystemMessagePromptTemplate.from_template(self.system_template)
        human_template = """User Question: {question}\n\nCurrency Pair: {pair}\nAnalysis: {analysis}"""
        human_message = HumanMessagePromptTemplate.from_template(human_template)
        
        chat_prompt = ChatPromptTemplate.from_messages([
            system_message,
            human_message
        ])
        
        messages = chat_prompt.format_messages(
            question=user_input,
            pair=currency_pair,
            analysis=analysis
        )
        
        response = self.llm(messages)
        return response.content
