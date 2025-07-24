#!/usr/bin/env python3
"""
Configuration module for environment variables
"""

import os
from typing import Set, Dict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_connection_string() -> str:
    """Get database connection string from environment variables"""
    required_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} environment variable is required")
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '5432')  # Default to 5432 if not specified
    db_name = os.getenv('DB_NAME')
    default_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return os.getenv('DATABASE_URL', default_url)

def get_bot_token() -> str:
    """Get Telegram bot token from environment variable"""
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")
    return token

def get_superadmin_ids() -> Set[str]:
    """Get superadmin IDs from environment variable"""
    admin_ids = os.getenv('TELEGRAM_SUPERADMIN_IDS', '')
    return {admin_id.strip() for admin_id in admin_ids.split(',') if admin_id.strip()}

def get_api_params() -> Dict[str, str]:
    """Get API parameters for DataFetcher from environment variables"""
    required_vars = [
        'API_COMPANY_ID', 'API_ITS_FROM_PERSOL', 'API_GROUP_BY', 'API_GROUP_BY1',
        'API_QUERY1', 'API_QUERY4', 'API_PIC_HEIGHT', 'API_PIC_WEIGHT',
        'API_PERIOD_ID', 'API_USER_ID', 'API_APP_ID'
    ]
    for var in required_vars:
        if os.getenv(var) is None:
            raise ValueError(f"{var} environment variable is required")
    
    return {
        'lngCompanyId': os.getenv('API_COMPANY_ID'),
        'szITSfromPersol': os.getenv('API_ITS_FROM_PERSOL'),
        'strGroupBy': os.getenv('API_GROUP_BY'),
        'strGroupBy1': os.getenv('API_GROUP_BY1'),
        'strQuery1': os.getenv('API_QUERY1'),
        'strQuery4': os.getenv('API_QUERY4'),
        'strPicHeight': os.getenv('API_PIC_HEIGHT'),
        'strPicWeight': os.getenv('API_PIC_WEIGHT'),
        'intPeriodID': os.getenv('API_PERIOD_ID'),
        'iUserId': os.getenv('API_USER_ID'),
        'iAppId': os.getenv('API_APP_ID')
    }