import sys
from pathlib import Path

from loguru import logger

# Create logs directory if it doesn't exist
log_path = Path("logs/app.log")
log_path.parent.mkdir(parents=True, exist_ok=True)

# Remove default handler
logger.remove()

# Sink 1: Stdout (Console)
# Output to sys.stderr with level="INFO".
# Format includes time, log level, module path, and message.
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
)

# Sink 2: File (JSON)
# Rotate every 500 MB, keep logs for 10 days, serialize to JSON, async enqueue.
logger.add("logs/app.log", rotation="500 MB", retention="10 days", serialize=True, enqueue=True)

__all__ = ["logger"]
