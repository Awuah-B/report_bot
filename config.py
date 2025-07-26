#!/usr/bin/env python3
"""
Configuration management for NPA Depot Manager Telegram Bot
Using Pydantic for validation and type safety
"""

import os
from enum import Enum
from typing import List
from pydantic_settings import BaseSettings
from pydantic import field_validator, HttpUrl
from dotenv import load_dotenv

load_dotenv()

class Environment(str, Enum):
    """Environment configuration enum"""
    DEVELOPMENT = "development"
    PRODUCTION = "production"

class DatabaseConfig(BaseSettings):
    """Database configuration"""
    supabase_url: HttpUrl
    supabase_anon_key: str
    supabase_realtime_url: HttpUrl | None = None

    @field_validator("supabase_url", "supabase_realtime_url")
    @classmethod
    def validate_urls(cls, v):
        if v is None:
            return v
        return str(v).rstrip("/")

class TelegramConfig(BaseSettings):
    """Telegram bot configuration"""
    bot_token: str
    superadmin_ids: List[int]
    webhook_url: HttpUrl | None = None
    webhook_port: int = 8443

    @field_validator("superadmin_ids")
    @classmethod
    def validate_superadmin_ids(cls, v):
        if not v:
            raise ValueError("At least one superadmin ID is required")
        return v

    @field_validator("bot_token")
    @classmethod
    def validate_bot_token(cls, v):
        if not v.startswith("bot"):
            raise ValueError("Bot token must start with 'bot'")
        return v

class APIConfig(BaseSettings):
    """API configuration"""
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

class Config(BaseSettings):
    """Main configuration class"""
    env: Environment = Environment.DEVELOPMENT
    log_level: str = "INFO"
    database: DatabaseConfig
    telegram: TelegramConfig
    api: APIConfig

    class Config:
        env_nested_delimiter = "__"

    # Legacy functions for backward compatibility
    def get_db_connection_string(self) -> str:
        import warnings
        warnings.warn(
            "get_db_connection_string is deprecated. Use CONFIG.database.supabase_url instead.",
            DeprecationWarning
        )
        return str(self.database.supabase_url)

    def get_bot_token(self) -> str:
        import warnings
        warnings.warn(
            "get_bot_token is deprecated. Use CONFIG.telegram.bot_token instead.",
            DeprecationWarning
        )
        return self.telegram.bot_token

    def get_superadmin_ids(self) -> List[int]:
        import warnings
        warnings.warn(
            "get_superadmin_ids is deprecated. Use CONFIG.telegram.superadmin_ids instead.",
            DeprecationWarning
        )
        return self.telegram.superadmin_ids

    def get_webhook_url(self) -> str | None:
        import warnings
        warnings.warn(
            "get_webhook_url is deprecated. Use CONFIG.telegram.webhook_url instead.",
            DeprecationWarning
        )
        return str(self.telegram.webhook_url) if self.telegram.webhook_url else None

    def is_production(self) -> bool:
        import warnings
        warnings.warn(
            "is_production is deprecated. Use CONFIG.env == Environment.PRODUCTION instead.",
            DeprecationWarning
        )
        return self.env == Environment.PRODUCTION

CONFIG = Config()