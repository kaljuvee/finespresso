from sqlalchemy import Column, Integer, String, TIMESTAMP
from utils.db.db_pool import DatabasePool
from datetime import datetime
import logging

# Get database pool instance
db_pool = DatabasePool()

class Signups(db_pool.Base):
    __tablename__ = 'signups'

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    captured_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

def save_email(email):
    with db_pool.get_session() as session:
        try:
            new_signup = Signups(email=email)
            session.add(new_signup)
            logging.info(f"Successfully added email: {email}")
            return True
        except Exception as e:
            logging.error(f"An error occurred while saving email: {e}")
            return False
