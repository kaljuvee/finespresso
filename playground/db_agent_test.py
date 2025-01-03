import os
import json
from datetime import datetime
import logging
from dotenv import load_dotenv
from ai.db_agent import DBAgent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def save_test_results(results, test_type):
    """Save test results to a JSON file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/{test_type}_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=4, default=str)
    
    logger.info(f"Test results saved to {filename}")

def test_db_agent():
    # Initialize DB Agent
    agent = DBAgent()
    
    # Test questions from Copilot Chat's news section
    test_questions = [
        "What are the latest merger-related predictions?",
        "Show me today's top 5 predicted stock moves",
        "Which companies have the highest predicted moves this week?",
        "List all acquisition predictions from last 3 days",
        "What are the trending market events today?",
        "Show me predictions for companies in the tech sector"
    ]
    
    # Store test results
    test_results = []
    
    # Process each question
    for question in test_questions:
        logger.info(f"Processing question: {question}")
        
        result = {
            "question": question,
            "timestamp": datetime.now(),
            "success": False,
            "error": None
        }
        
        try:
            query, results = agent.process_question(question)
            
            result.update({
                "success": True,
                "query": query,
                "results": results.to_dict() if results is not None else None,
                "row_count": len(results) if results is not None else 0
            })
            
            logger.info(f"Successfully processed question. Query: {query}")
            logger.info(f"Results shape: {results.shape if results is not None else 'No results'}")
            
        except Exception as e:
            error_msg = f"Error processing question: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
        
        test_results.append(result)
    
    # Save test results
    save_test_results(test_results, "db_agent_test")
    
    # Print summary
    logger.info("\nTest Summary:")
    successful_tests = sum(1 for r in test_results if r["success"])
    logger.info(f"Total tests: {len(test_results)}")
    logger.info(f"Successful: {successful_tests}")
    logger.info(f"Failed: {len(test_results) - successful_tests}")

if __name__ == "__main__":
    test_db_agent() 