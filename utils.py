#!/usr/bin/env python3
"""
Utility module for shared functionality
"""

import logging
import os

def setup_logging(log_file: str) -> logging.Logger:
    """Setup logging configuration"""
    logger = logging.getLogger(__name__)
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.handlers = [file_handler, stream_handler]
    return logger