from langchain_openai import ChatOpenAI
from langchain.prompts import SystemMessagePromptTemplate, HumanMessagePromptTemplate, ChatPromptTemplate
from langchain.schema import SystemMessage, HumanMessage
from langchain_experimental.agents.agent_toolkits.pandas.base import create_pandas_dataframe_agent
import os
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import re
import json
from ai.base_agent import BaseAgent
from ai.utils.prompt_util import get_prompt_by_name
from ai.utils.logger_util import setup_logger

logger = setup_logger(__name__)

class MarketAgent(BaseAgent):
    def __init__(self):
        super().__init__(temperature=0.7)
        self.timeframe_prompt = get_prompt_by_name("timeframe_parser")
        logger.info("MarketAgent initialized")
        
    def parse_timeframe(self, text):
        """Use LLM to parse timeframe from user input"""
        logger.debug(f"Parsing timeframe from: {text}")
        
        # Get formatted prompt using template
        formatted_prompt = get_prompt_by_name("timeframe_parser", {"text": text})
        logger.debug(f"Using timeframe prompt: {formatted_prompt}")
        
        messages = [
            SystemMessage(content=self.system_template),
            HumanMessage(content=formatted_prompt)
        ]
        
        try:
            response = self.llm.invoke(messages)
            logger.debug(f"Raw LLM response: {response.content}")
            
            # Try to clean the response if it contains extra text
            content = response.content.strip()
            if not content.startswith('{'):
                # Try to find JSON in the response
                import re
                json_match = re.search(r'\{.*\}', content)
                if json_match:
                    content = json_match.group()
                    logger.debug(f"Extracted JSON from response: {content}")
                else:
                    logger.error(f"Could not find JSON in response: {content}")
                    return timedelta(hours=24)
            
            timeframe_data = json.loads(content)
            logger.info(f"Successfully parsed timeframe data: {timeframe_data}")
            
            # Convert parsed timeframe to timedelta
            unit = timeframe_data['unit']
            amount = timeframe_data['amount']
            
            logger.debug(f"Parsed timeframe: {amount} {unit}")
            
            if unit == 'hours':
                return timedelta(hours=amount)
            elif unit == 'days':
                return timedelta(days=amount)
            elif unit == 'weeks':
                return timedelta(days=amount * 7)
            elif unit == 'months':
                return timedelta(days=amount * 30)
            else:
                logger.warning(f"Unknown time unit: {unit}, using default")
                return timedelta(hours=24)  # Default fallback
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            logger.error(f"Failed to parse response content: {response.content}")
            return timedelta(hours=24)
        except Exception as e:
            logger.error(f"Error parsing timeframe: {e}", exc_info=True)
            return timedelta(hours=24)  # Default fallback
            
    def get_currency_data(self, currency_pair, user_input):
        """
        Fetch currency rate data from Yahoo Finance
        
        Args:
            currency_pair (str): Currency pair in format 'XXX/YYY'
            user_input (str): User query containing timeframe information
            
        Returns:
            pd.DataFrame: DataFrame with rate data
        """
        # Get timeframe using LLM
        timeframe = self.parse_timeframe(user_input)
        
        # Format currency pair exactly like rate_util.py does
        ticker_symbol = currency_pair.replace('/', '') + '=X'
        logger.info(f"Fetching data for {ticker_symbol} from {timeframe} ago")
        
        # Use UTC for consistent timezone handling
        end_date = pd.Timestamp.now(tz='UTC')
        start_date = end_date - timeframe
        
        # Adjust interval based on timeframe for better data granularity
        if timeframe <= timedelta(hours=1):
            interval = "1m"  # Use 1-minute data for hour or less
            # For very recent data, adjust start time to ensure we get data
            start_date = end_date - timedelta(hours=2)  # Get 2 hours of data
        elif timeframe <= timedelta(hours=24):
            interval = "5m"  # Use 5-minute data for up to 24 hours
            start_date = end_date - timedelta(days=1)
        elif timeframe <= timedelta(days=7):
            interval = "1h"
            # Add buffer for hourly data
            start_date = end_date - timedelta(days=8)
        else:
            interval = "1d"
            # Add buffer for daily data
            start_date = end_date - timeframe - timedelta(days=5)
        
        logger.debug(f"Using interval: {interval} for timeframe: {timeframe}")
        logger.debug(f"Adjusted date range: {start_date} to {end_date}")
        
        def fetch_data(symbol, inverse=False):
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(start=start_date, end=end_date, interval=interval)
                
                if not data.empty:
                    if inverse:
                        # Invert rates for inverse pair
                        for col in ['Open', 'High', 'Low', 'Close']:
                            data[col] = 1 / data[col]
                        logger.info(f"Successfully fetched and inverted data for {symbol}")
                    else:
                        logger.info(f"Successfully fetched data for {symbol}")
                    
                    # Filter to requested timeframe using localization-aware comparison
                    filter_start = end_date - timeframe
                    mask = (data.index >= filter_start) & (data.index <= end_date)
                    filtered_data = data[mask]
                    
                    # Check if we have data after filtering
                    if filtered_data.empty:
                        logger.warning(f"No data available after filtering for {symbol}")
                        return pd.DataFrame()
                    
                    logger.debug(f"Data shape after filtering: {filtered_data.shape}")
                    logger.debug(f"Time range: {filtered_data.index[0]} to {filtered_data.index[-1]}")
                    logger.debug(f"Latest rates: Open={filtered_data['Open'].iloc[-1]:.4f}, Close={filtered_data['Close'].iloc[-1]:.4f}")
                    return filtered_data
                
                logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
                
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}", exc_info=True)
                return pd.DataFrame()
        
        # Try primary pair
        data = fetch_data(ticker_symbol)
        if not data.empty:
            return data
            
        # Try inverse pair
        logger.info(f"Attempting to fetch inverse pair data")
        base, quote = currency_pair.split('/')
        inverse_pair = f"{quote}/{base}"
        inverse_symbol = inverse_pair.replace('/', '') + '=X'
        
        data = fetch_data(inverse_symbol, inverse=True)
        if not data.empty:
            return data
            
        logger.error(f"Failed to fetch data for both {ticker_symbol} and {inverse_symbol}")
        return pd.DataFrame()
        
    def perform_analysis(self, df, question):
        """Use pandas DataFrame agent to analyze the rate data"""
        if df.empty:
            return "No data available for analysis."
            
        try:
            # Create pandas DataFrame agent with updated parameters
            agent = create_pandas_dataframe_agent(
                llm=self.llm,
                df=df,
                verbose=True,
                agent_type="openai-tools",
                handle_parsing_errors=True,
                allow_dangerous_code=True,
                max_iterations=5,
                max_execution_time=30,
            )
            
            # More specific analysis prompt
            analysis_prompt = """
            Using the provided DataFrame, please analyze the following:
            1. Calculate the percentage change between the first and last Close price
            2. Calculate the average price for the period
            3. Find the highest and lowest prices
            4. Determine the overall trend (up/down/sideways)
            
            DataFrame info:
            - The data contains OHLC prices
            - Index is datetime
            - First Close: {first_close:.4f}
            - Last Close: {last_close:.4f}
            - Time range: {start_time} to {end_time}
            
            Original question: {question}
            
            Format the response in a clear, readable way.
            """.format(
                first_close=df['Close'].iloc[0],
                last_close=df['Close'].iloc[-1],
                start_time=df.index[0],
                end_time=df.index[-1],
                question=question
            )
            
            response = agent.run(analysis_prompt)
            return response
        except Exception as e:
            logger.error(f"Error in perform_analysis: {str(e)}", exc_info=True)
            return f"Error performing analysis: {str(e)}"
        
    def normalize_currency_pair(self, text):
        # Common currency codes in priority order
        currencies = ['GBP', 'EUR', 'USD', 'JPY', 'AUD', 'CAD', 'CHF', 'NZD']
        
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
            # Use priority order instead of alphabetical
            first_curr = next(curr for curr in currencies if curr in mentioned_currencies)
            second_curr = next(curr for curr in mentioned_currencies if curr != first_curr)
            return f"{first_curr}/{second_curr}"

    def process_query(self, user_input):
        """Process a user query about currency markets"""
        try:
            # Step 1: Extract currency pair
            currency_pair = self.normalize_currency_pair(user_input)
            if not currency_pair:
                return "I couldn't identify any currency pairs in your question. Please mention specific currencies."
            
            # Step 2: Get currency data
            data = self.get_currency_data(currency_pair, user_input)
            if data.empty:
                return f"I couldn't fetch the rate data for {currency_pair}. Please try again with a different timeframe or currency pair."
            
            # Step 3: Perform analysis
            analysis = self.perform_analysis(data, user_input)
            if not analysis:
                return "I was unable to analyze the data. Please try rephrasing your question."
            
            # Step 4: Format response
            response = f"""
Analysis for {currency_pair}:
Time period: {data.index[0]} to {data.index[-1]}
Latest rate: {data['Close'].iloc[-1]:.4f}

{analysis}
"""
            return response
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}", exc_info=True)
            return f"I encountered an error while processing your request: {str(e)}"
