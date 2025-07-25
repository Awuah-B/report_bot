#!/usr/bin/env python3
"""
Configuration module for NPA data fetching application
Updated for Supabase integration
"""

import os
from typing import Set, Dict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_connection_string() -> str:
    """Get database connection string for Supabase"""
    # For production (Render), use environment variable
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return db_url

    # For local development, construct from individual components
    db_host = os.getenv('DB_HOST', 'aws-0-eu-north-1.pooler.supabase.com')
    db_port = os.getenv('DB_PORT', '6543')
    db_name = os.getenv('DB_NAME', 'postgres')
    db_user = os.getenv('DB_USER', 'postgres.aobymozhvggoaffsygot')
    db_password = os.getenv('DB_PASSWORD')

    if not db_password:
        raise ValueError("Databse password not fouund in environment variables")
    return f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    

def get_bot_token() -> str:
    """Get Telegram bot token"""
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not token:
        raise ValueError("Telegram bot token not found in environment variables")
    return token

def get_superadmin_ids() -> Set[str]:
    """Get superadmin IDs"""
    ids = os.getenv('TELEGRAM_SUPERADMIN_ID', '')
    return [id.strip() for id in ids.split(',') if id.strip()]

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
m = get_db_connection_string()
print(m)