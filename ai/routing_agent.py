from typing import Dict, Any, Optional
import json
from utils.logging.log_util import get_logger
from .market_agent import MarketAgent
from .db_agent import DBAgent

logger = get_logger(__name__)

class RoutingAgent:
    def __init__(self):
        """Initialize routing agent with available agents"""
        self.market_agent = MarketAgent()
        self.db_agent = DBAgent()
        logger.info("Routing agent initialized with market and DB agents")

    def route_query(self, query: str) -> Dict[str, Any]:
        """
        Route the query to appropriate agent based on prefix
        Returns formatted response dictionary
        """
        try:
            query = query.strip()
            logger.info(f"Routing query: {query}")

            # Check for prefixes
            if query.lower().startswith("market:"):
                logger.info("Routing to market agent")
                response = self.market_agent.process_financial_query(query[7:].strip())
                return json.loads(response)
                
            elif query.lower().startswith("news:"):
                logger.info("Routing to DB agent")
                stripped_query = query[5:].strip()
                logger.info(f"Stripped query for DB agent: {stripped_query}")
                query, results = self.db_agent.process_question(stripped_query)
                
                if results is not None and not results.empty:
                    logger.info(f"DB query returned {len(results)} rows")
                    return {
                        "response": f"Here are the results:\n\n{results.to_markdown()}",
                        "success": True
                    }
                else:
                    logger.warning("DB query returned no results")
                    return {
                        "response": "No results found for your query.",
                        "success": True
                    }
            else:
                # Default to market agent if no prefix
                logger.info("No prefix found, defaulting to market agent")
                response = self.market_agent.process_financial_query(query)
                return json.loads(response)

        except Exception as e:
            logger.error(f"Error routing query: {str(e)}")
            return {
                "response": f"Error processing query: {str(e)}",
                "success": False
            }

    def set_model(self, model_name: str):
        """Update model for market agent"""
        self.market_agent.set_model(model_name)

    def toggle_plotly(self, use_plotly: bool):
        """Toggle plotly for market agent"""
        self.market_agent.toggle_plotly(use_plotly) 