from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text
from utils.db_pool import db_pool

Base = db_pool.get_base()

class Conversation(Base):
    __tablename__ = 'conversations'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String(100))
    session_id = Column(String(100))
    timestamp = Column(DateTime, default=datetime.utcnow)
    user_prompt = Column(Text)
    answer = Column(Text)

def create_tables():
    Base.metadata.create_all(db_pool.get_engine())

def store_conversation(user_id, session_id, user_prompt, answer):
    session = db_pool.get_session()
    try:
        conversation = Conversation(
            user_id=user_id,
            session_id=session_id,
            user_prompt=user_prompt,
            answer=answer
        )
        session.add(conversation)
        session.commit()
        return conversation
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def get_conversation_history(session_id):
    session = db_pool.get_session()
    try:
        conversations = session.query(Conversation)\
            .filter(Conversation.session_id == session_id)\
            .order_by(Conversation.timestamp)\
            .all()
        return conversations
    finally:
        session.close()
