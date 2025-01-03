from abc import ABC, abstractmethod
import os
from utils.logging.log_util import get_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = get_logger(__name__)

class BaseAgent(ABC):
    """Base class for all agents"""
    
    def __init__(self, openai_api_key=None):
        """Initialize base agent with OpenAI API key"""
        # Use environment variable by default
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
            
        # Get default model from environment
        self.default_model = os.getenv("OPENAI_MODEL", "gpt-4-turbo-preview")
        
        os.environ["OPENAI_API_KEY"] = self.openai_api_key
        self._initialize()
        logger.info(f"Initialized {self.__class__.__name__}")

    @abstractmethod
    def _initialize(self):
        """Initialize agent-specific components"""
        pass

    @abstractmethod
    def process_question(self, question):
        """Process a question and return results"""
        pass 