from langchain_openai import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
import os
from dotenv import load_dotenv
from ai.utils.prompt_util import get_prompt_by_name

class BaseAgent:
    def __init__(self, model_name=None, temperature=0.7):
        """Initialize the base agent with a LangChain LLM client"""
        # Load environment variables
        load_dotenv()
        
        # Get API key and model from environment
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
            
        self.model_name = model_name or os.getenv("OPENAI_MODEL", "gpt-4")
        
        self.llm = ChatOpenAI(
            model_name=self.model_name,
            temperature=temperature,
            openai_api_key=api_key
        )
        
        # Load system prompt if exists
        self.system_template = self._load_system_prompt()
    
    def _load_system_prompt(self):
        """Load system prompt from file if it exists"""
        try:
            return get_prompt_by_name(self.__class__.__name__.lower())
        except FileNotFoundError:
            return ""
            
    def query(self, messages):
        """Send a query to the LLM"""
        try:
            response = self.llm.invoke(messages)
            return response
        except Exception as e:
            print(f"Error querying LLM: {e}")
            return None 