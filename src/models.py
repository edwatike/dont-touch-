from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ParsedSite(Base):
    """Модель для хранения результатов парсинга"""
    __tablename__ = 'parsed_sites'
    
    id = Column(Integer, primary_key=True)
    url = Column(String(500), unique=True)
    title = Column(String(500))
    description = Column(Text)
    parsed_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50))
    error = Column(Text, nullable=True)
