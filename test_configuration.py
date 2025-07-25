#!/usr/bin/env python3
from config import get_db_connection_string
from sqlalchemy import create_engine, text
from utils import setup_logging

logger = setup_logging('test_db.log')

def test_connection():
    try:
        engine = create_engine(get_db_connection_string())
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            logger.info(f"Connected to: {result.scalar()}")
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name LIKE '%depot_manager%'
            """))
            tables = [row[0] for row in result.fetchall()]
            logger.info(f"Depot Manager tables: {tables}")
        return True
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()