#!/usr/bin/env python3
"""
Enhanced Utility Module with Additional Functionality
"""

import logging
import os
import sys
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import json
import pandas as pd
import hashlib

def setup_logging(log_file: str, logger_name: str = __name__) -> logging.Logger:
    """Enhanced logging setup with rotation and environment awareness"""
    logger = logging.getLogger(logger_name)
    
    # Clear existing handlers to avoid duplicate logs
    if logger.handlers:
        logger.handlers.clear()
    
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler with rotation
    try:
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Failed to setup file logging: {e}", file=sys.stderr)
    
    # Stream handler for console output
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    return logger

def validate_environment_vars(required_vars: List[str]) -> bool:
    """Validate required environment variables"""
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger = setup_logging('env_validation.log')
        logger.error(f"Missing required environment variables: {missing}")
        return False
    return True

def parse_config_file(config_path: str) -> Optional[Dict[str, Any]]:
    """Safely parse JSON configuration file"""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger = setup_logging('config_parser.log')
        logger.error(f"Invalid JSON in config file {config_path}: {e}")
    except Exception as e:
        logger.error(f"Failed to read config file {config_path}: {e}")
    return None

def dataframe_to_dict(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert DataFrame to list of dictionaries with proper type handling"""
    if df.empty:
        return []
    
    # Convert datetime columns to ISO format strings
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    for col in datetime_cols:
        df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    
    # Convert numeric columns to native Python types
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) and x.is_integer() else float(x) if pd.notnull(x) else None)
    
    return df.replace({pd.NA: None}).to_dict('records')

def format_timedelta(delta: timedelta) -> str:
    """Format timedelta into human-readable string"""
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    
    return ' '.join(parts)

def create_data_hash(data: Any) -> str:
    """Create consistent hash from data"""
    if isinstance(data, pd.DataFrame):
        data = dataframe_to_dict(data)
    
    data_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode()).hexdigest()

def setup_database_logging() -> logging.Logger:
    """Specialized logger for database operations"""
    db_logger = logging.getLogger('supabase')
    db_logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s - DB - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    handler = logging.FileHandler('database_operations.log')
    handler.setFormatter(formatter)
    db_logger.addHandler(handler)
    
    return db_logger

def validate_dataframe_structure(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """Validate DataFrame has required columns"""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logger = setup_logging('data_validation.log')
        logger.error(f"DataFrame missing required columns: {missing}")
        return False
    return True

def backup_file(file_path: str) -> bool:
    """Create timestamped backup of a file"""
    if not os.path.exists(file_path):
        return False
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{file_path}.bak_{timestamp}"
    
    try:
        import shutil
        shutil.copy2(file_path, backup_path)
        return True
    except Exception as e:
        logger = setup_logging('file_operations.log')
        logger.error(f"Failed to backup {file_path}: {e}")
        return False

if __name__ == "__main__":
    # Test the utility functions
    logger = setup_logging('utils_test.log')
    logger.info("Testing utility functions")
    
    # Test environment validation
    required_vars = ['SUPABASE_URL', 'SUPABASE_ANON_KEY']
    logger.info(f"Environment validation: {validate_environment_vars(required_vars)}")
    
    # Test DataFrame conversion
    test_df = pd.DataFrame({
        'date': [datetime.now(), datetime.now() - timedelta(days=1)],
        'value': [1.5, 2],
        'text': ['test', None]
    })
    logger.info(f"DataFrame conversion test: {dataframe_to_dict(test_df)}")