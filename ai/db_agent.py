import streamlit as st
from langchain.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from langchain.llms import OpenAI
import pandas as pd
from utils.logging.log_util import get_logger
from utils.db.db_pool import DatabasePool
from ai.base_agent import BaseAgent

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
            self.db_chain = create_sql_query_chain(self.llm, self.db)
            logger.info("Successfully initialized DBAgent components")
        except Exception as e:
            logger.error(f"Error initializing DBAgent components: {str(e)}")
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
                result = session.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                logger.info(f"Successfully executed query, returned {len(df)} rows")
                return df
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def process_question(self, question):
        """Process natural language question and return results."""
        try:
            query = self.generate_query(question)
            results = self.execute_query(query)
            return query, results
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}")
            raise
