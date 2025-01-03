import streamlit as st
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from langchain_openai import OpenAI
import pandas as pd
from utils.logging.log_util import get_logger
from utils.db.db_pool import DatabasePool
from ai.base_agent import BaseAgent
import os

logger = get_logger(__name__)

class DBAgent(BaseAgent):
    def _initialize(self):
        """Initialize DB Agent specific components."""
        try:
            # Get database pool instance
            self.db_pool = DatabasePool()
            
            # Initialize LangChain components
            self.db = SQLDatabase.from_uri(self.db_pool.engine.url)
            self.llm = OpenAI(temperature=0, verbose=True)
            
            # Create SQL chain without prompt template
            self.db_chain = create_sql_query_chain(self.llm, self.db)
            
            logger.info("Successfully initialized DBAgent components")
        except Exception as e:
            logger.error(f"Error initializing DBAgent components: {str(e)}")
            raise

    def load_prompts(self):
        """Deprecated - using database schema instead"""
        pass

    def format_predictions_df(self, df):
        """Format predictions dataframe for display"""
        # Select and rename relevant columns
        display_columns = {
            'title': 'Title',
            'company': 'Company',
            'predicted_move': 'Expected Move (%)',
            'event': 'Event',
            'reason': 'Reason',
            'published_date': 'Published Date'
        }
        
        formatted_df = df[display_columns.keys()].copy()
        formatted_df.rename(columns=display_columns, inplace=True)
        
        # Format numeric columns
        if 'Expected Move (%)' in formatted_df.columns:
            formatted_df['Expected Move (%)'] = formatted_df['Expected Move (%)'].apply(
                lambda x: f"{x:.2f}%" if pd.notnull(x) else ''
            )
            
        # Format datetime
        if 'Published Date' in formatted_df.columns:
            formatted_df['Published Date'] = pd.to_datetime(
                formatted_df['Published Date']
            ).dt.strftime('%Y-%m-%d %H:%M')
            
        return formatted_df

    def get_top_predictions(self, limit=5, days=1):
        """Get top predicted moves"""
        query = """
        SELECT title, company, predicted_move, event, reason, published_date
        FROM news
        WHERE published_date >= NOW() - INTERVAL :days DAY
        AND predicted_move IS NOT NULL
        ORDER BY ABS(predicted_move) DESC
        LIMIT :limit
        """
        
        try:
            with self.db_pool.get_session() as session:
                result = session.execute(
                    query, 
                    {'limit': limit, 'days': days}
                )
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return self.format_predictions_df(df)
        except Exception as e:
            logger.error(f"Error getting top predictions: {str(e)}")
            raise

    def get_predictions_by_topic(self, days=3):
        """Get predictions grouped by topic/event"""
        query = """
        SELECT 
            event,
            COUNT(*) as count,
            AVG(predicted_move) as avg_move,
            MIN(predicted_move) as min_move,
            MAX(predicted_move) as max_move
        FROM news
        WHERE published_date >= NOW() - INTERVAL :days DAY
        AND predicted_move IS NOT NULL
        GROUP BY event
        ORDER BY count DESC
        """
        
        try:
            with self.db_pool.get_session() as session:
                result = session.execute(query, {'days': days})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                # Format numeric columns
                for col in ['avg_move', 'min_move', 'max_move']:
                    df[col] = df[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else '')
                    
                return df
        except Exception as e:
            logger.error(f"Error getting predictions by topic: {str(e)}")
            raise

    def generate_query(self, question):
        """Generate SQL query from natural language question."""
        try:
            result = self.db_chain.invoke({"question": question})
            logger.info(f"Generated SQL query for question: {question}")
            return result
        except Exception as e:
            logger.error(f"Error generating query: {str(e)}")
            raise

    def execute_query(self, query):
        """Execute SQL query and return results as DataFrame."""
        try:
            with self.db_pool.get_session() as session:
                logger.info(f"Executing query: {query}")
                result = session.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                logger.info(f"Successfully executed query, returned {len(df)} rows")
                if len(df) == 0:
                    logger.warning("Query returned no results")
                return df
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def get_prediction_queries(self, question: str) -> str:
        """Generate appropriate SQL query based on prediction-related questions"""
        
        if "merger" in question.lower() or "acquisition" in question.lower():
            return """
            SELECT 
                title,
                company,
                predicted_move as "Expected Move (%)",
                event,
                reason,
                published_date
            FROM news 
            WHERE event IN ('Mergers', 'Acquisitions', 'M&A')
            AND published_date >= NOW() - INTERVAL '3 days'
            AND predicted_move IS NOT NULL
            ORDER BY ABS(predicted_move) DESC
            LIMIT 10
            """
        
        if "top 5" in question.lower() or "highest" in question.lower():
            return """
            SELECT 
                title,
                company,
                predicted_move as "Expected Move (%)",
                event,
                reason,
                published_date
            FROM news
            WHERE published_date >= CURRENT_DATE
            AND predicted_move IS NOT NULL
            ORDER BY ABS(predicted_move) DESC
            LIMIT 5
            """
        
        if "tech sector" in question.lower():
            return """
            SELECT 
                title,
                company,
                predicted_move as "Expected Move (%)",
                event,
                reason,
                published_date
            FROM news
            WHERE industry = 'Technology'
            AND published_date >= NOW() - INTERVAL '3 days'
            AND predicted_move IS NOT NULL
            ORDER BY published_date DESC
            LIMIT 10
            """
        
        # Default prediction query
        return """
        SELECT 
            title,
            company,
            predicted_move as "Expected Move (%)",
            event,
            reason,
            published_date
        FROM news
        WHERE published_date >= NOW() - INTERVAL '1 day'
        AND predicted_move IS NOT NULL
        ORDER BY published_date DESC
        LIMIT 10
        """

    def process_question(self, question):
        """Process natural language question and return results."""
        try:
            logger.info(f"Processing question: {question}")
            
            # Handle specific prediction queries
            if "highest" in question.lower() and "predicted" in question.lower():
                query = """
                SELECT 
                    title,
                    company,
                    predicted_move as "Expected Move (%)",
                    event,
                    reason,
                    published_date
                FROM news
                WHERE published_date >= NOW() - INTERVAL '7 days'  -- This week
                AND predicted_move IS NOT NULL
                ORDER BY ABS(predicted_move) DESC
                LIMIT 10
                """
                logger.info(f"Using highest predictions query: {query}")
                results = self.execute_query(query)
                logger.info(f"Query returned {len(results)} rows")
                return query, self.format_predictions_df(results)
            
            # Check if it's a prediction-related question
            elif any(word in question.lower() for word in ['prediction', 'merger', 'acquisition', 'move']):
                query = self.get_prediction_queries(question)
                logger.info(f"Generated prediction query: {query}")
                results = self.execute_query(query)
                logger.info(f"Query returned {len(results)} rows")
                return query, self.format_predictions_df(results)
            
            # Generate SQL for other queries
            else:
                query = self.generate_query(question)
                logger.info(f"Generated general query: {query}")
                results = self.execute_query(query)
                logger.info(f"Query returned {len(results)} rows")
                return query, results
            
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}")
            raise
