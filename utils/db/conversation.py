from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text
from utils.db.db_pool import DatabasePool

# Get database pool instance
db_pool = DatabasePool()
Base = db_pool.Base

class Conversation(Base):
    __tablename__ = 'conversations'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String(100))
    session_id = Column(String(100))
    timestamp = Column(DateTime, default=datetime.utcnow)
    user_prompt = Column(Text)
    answer = Column(Text)

def create_tables():
    db_pool.create_all_tables()

def store_conversation(user_id, session_id, user_prompt, answer):
    with db_pool.get_session() as session:
        conversation = Conversation(
            user_id=user_id,
            session_id=session_id,
            user_prompt=user_prompt,
            answer=answer
        )
        session.add(conversation)
        return conversation

def get_conversation_history(session_id):
    with db_pool.get_session() as session:
        conversations = session.query(Conversation)\
            .filter(Conversation.session_id == session_id)\
            .order_by(Conversation.timestamp)\
            .all()
        return conversations 