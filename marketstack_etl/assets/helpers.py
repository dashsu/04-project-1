from sqlalchemy import MetaData
from loguru import logger
import os
import sys
from datetime import timedelta

LOG_DIR = "logs"

def get_schema_metadata(engine):
    # Get the metadata of a DB
    metadata = MetaData(bind=engine)
    metadata.reflect() 
    return metadata


def setup_logger():

    logger.remove()

    # Print logs into console
    console_fmt = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}:{function}:{line}</cyan> | "
        "{message} {extra}"
    )

    # Add console logging capabilities
    logger.add(
        sys.stderr,
        level="DEBUG",
        diagnose=False,
        enqueue=True,
        format=console_fmt,
    )

    # Also save information into a JSON
    logger.add(
        os.path.join(LOG_DIR, "app_{time:YYYY-MM-DD_HH}.jsonl"),
        level="DEBUG",
        rotation=timedelta(hours=1),
        retention="7 days",
        compression="zip",
        enqueue=True,
        serialize=True
    )

    return logger.bind()
