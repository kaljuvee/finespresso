import os
from dotenv import load_dotenv
from ai.db_agent import DBAgent

# Load environment variables
load_dotenv()

# Get OpenAI API key from environment variables
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

def test_db_agent():
    # Initialize DB Agent
    agent = DBAgent(OPENAI_API_KEY)
    
    # Test questions
    test_questions = [
        "What are the latest 5 news articles about Tesla?",
        "Show me news articles with positive predicted_side from last week",
        "What companies had the most news articles in the technology industry?",
        "Find articles where predicted_move is greater than 5%",
        "What are the most common events in the database?"
    ]
    
    # Process each question
    for question in test_questions:
        print(f"\nQuestion: {question}")
        try:
            query, results = agent.process_question(question)
            print("\nGenerated SQL:")
            print(query)
            print("\nResults:")
            print(results.head())
        except Exception as e:
            print(f"Error processing question: {str(e)}")

if __name__ == "__main__":
    test_db_agent() 