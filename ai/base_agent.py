from abc import ABC, abstractmethod
import os
from utils.logging.log_util import get_logger

logger = get_logger(__name__)

class BaseAgent(ABC):
    """Base class for all agents"""
    
    def __init__(self, openai_api_key):
        """Initialize base agent with OpenAI API key"""
        self.openai_api_key = openai_api_key
        os.environ["OPENAI_API_KEY"] = openai_api_key
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