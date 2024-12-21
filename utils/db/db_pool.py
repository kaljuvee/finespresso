from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os
from contextlib import contextmanager
from utils.logging.log_util import get_logger

logger = get_logger(__name__)

class DatabasePool:
    _instance = None
    _engine = None
    _SessionFactory = None
    Base = declarative_base()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize database connection pool"""
        load_dotenv()
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set")

        # Configure connection pool
        self._engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=5,  # Maximum number of connections in the pool
            max_overflow=10,  # Maximum number of connections that can be created beyond pool_size
            pool_timeout=30,  # Timeout for getting a connection from the pool
            pool_recycle=1800,  # Recycle connections after 30 minutes
            pool_pre_ping=True  # Enable connection health checks
        )
        
        self._SessionFactory = sessionmaker(bind=self._engine)
        logger.info("Database connection pool initialized")

    @property
    def engine(self):
        """Get the SQLAlchemy engine"""
        return self._engine

    @property
    def SessionFactory(self):
        """Get the session factory"""
        return self._SessionFactory

    @contextmanager
    def get_session(self):
        """Get a database session from the pool with context management"""
        session = self._SessionFactory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            session.close()

    def create_all_tables(self):
        """Create all tables defined in the models"""
        self.Base.metadata.create_all(self._engine)
        logger.info("All database tables created")

    def drop_all_tables(self):
        """Drop all tables defined in the models"""
        self.Base.metadata.drop_all(self._engine)
        logger.info("All database tables dropped") 