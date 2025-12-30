# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import os
import sys
from loguru import logger

# Remove default handler
logger.remove()

# Sink 1 (Stdout)
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)

# Sink 2 (File)
# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

logger.add(
    "logs/app.log",
    rotation="500 MB",
    retention="10 days",
    serialize=True,
    enqueue=True,
    level="DEBUG",
)

# Export the configured logger
__all__ = ["logger"]
