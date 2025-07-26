#!/usr/bin/env python3
"""
Enhanced Configuration Management for NPA Depot Manager System
Features:
- Type-safe configuration
- Environment validation
- Default values
- Configuration groups
- Error handling
"""

import os
from typing import Set, Dict, List, Optional
from dotenv import load_dotenv
from pydantic import BaseSettings, field_validator, HttpUrl
from enum import Enum
import logging

# Initialize logging
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class Environment(str, Enum):
    """Application environment types"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"

class DatabaseConfig(BaseSettings):
    """Database configuration model"""
    url: HttpUrl
    anon_key: str
    service_key: Optional[str] = None
    pool_size: int = 5
    timeout: int = 30

    class Config:
        env_prefix = "SUPABASE_"
        fields = {
            "anon_key": {"env": "SUPABASE_ANON_KEY"},
            "service_key": {"env": "SUPABASE_SERVICE_KEY"}
        }

class APIConfig(BaseSettings):
    """API configuration model"""
    company_id: str
    its_from_persol: str
    group_by: str
    group_by1: str
    query1: str
    query4: str
    pic_height: str
    pic_weight: str
    period_id: str
    user_id: str
    app_id: str

    class Config:
        env_prefix = "API_"

class TelegramConfig(BaseSettings):
    """Telegram bot configuration"""
    bot_token: str
    superadmin_ids: List[int]
    webhook_url: Optional[str] = None
    webhook_port: int = 8443

    @field_validator('superadmin_ids')
    @classmethod
    def parse_superadmin_ids(cls, v):
        """Validate and parse superadmin_ids from comma-separated string or list"""
        if isinstance(v, str):
            try:
                return [int(id.strip()) for id in v.split(',') if id.strip()]
            except ValueError as e:
                logger.error(f"Invalid superadmin_ids format: {v}. Expected comma-separated integers.")
                raise ValueError("superadmin_ids must be comma-separated integers")
        return v

    class Config:
        env_prefix = "TELEGRAM_"

class AppConfig(BaseSettings):
    """Main application configuration"""
    env: Environment = Environment.DEVELOPMENT
    debug: bool = False
    log_level: str = "INFO"
    timezone: str = "UTC"

    database: DatabaseConfig
    api: APIConfig
    telegram: TelegramConfig

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {', '.join(valid_levels)}")
        return v.upper()

    class Config:
        env_nested_delimiter = "__"

def load_config() -> AppConfig:
    """Load and validate application configuration"""
    try:
        config = AppConfig(
            database=DatabaseConfig(),
            api=APIConfig(),
            telegram=TelegramConfig()
        )
        logger.info(f"Configuration loaded for {config.env.value} environment")
        return config
    except Exception as e:
        logger.critical(f"Failed to load configuration: {str(e)}")
        raise

# Configuration singleton
CONFIG = load_config()

# Legacy functions for backward compatibility
def get_db_connection_string() -> str:
    """Legacy function for database connection string"""
    logger.warning("get_db_connection_string() is deprecated. Use CONFIG.database.url instead")
    return str(CONFIG.database.url)

def get_bot_token() -> str:
    """Legacy function for Telegram bot token"""
    logger.warning("get_bot_token() is deprecated. Use CONFIG.telegram.bot_token instead")
    return CONFIG.telegram.bot_token

def get_superadmin_ids() -> Set[str]:
    """Legacy function for superadmin IDs"""
    logger.warning("get_superadmin_ids() is deprecated. Use CONFIG.telegram.superadmin_ids instead")
    return {str(id) for id in CONFIG.telegram.superadmin_ids}

def get_api_params() -> Dict[str, str]:
    """Legacy function for API parameters"""
    logger.warning("get_api_params() is deprecated. Use CONFIG.api instead")
    api = CONFIG.api
    return {
        'lngCompanyId': api.company_id,
        'szITSfromPersol': api.its_from_persol,
        'strGroupBy': api.group_by,
        'strGroupBy1': api.group_by1,
        'strQuery1': api.query1,
        'strQuery4': api.query4,
        'strPicHeight': api.pic_height,
        'strPicWeight': api.pic_weight,
        'intPeriodID': api.period_id,
        'iUserId': api.user_id,
        'iAppId': api.app_id
    }

def get_environment() -> Environment:
    """Get current application environment"""
    return CONFIG.env

def is_production() -> bool:
    """Check if running in production environment"""
    return CONFIG.env == Environment.PRODUCTION

def is_debug() -> bool:
    """Check if debug mode is enabled"""
    return CONFIG.debug