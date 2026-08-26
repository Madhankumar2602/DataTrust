"""
logger.py — Shared logging configuration for DataTrust.

What is logging and why do we need it?
───────────────────────────────────────
`print()` is fine for quick experiments, but in a real system:
  - You want timestamps so you know WHEN something happened.
  - You want severity levels (DEBUG/INFO/WARNING/ERROR) so you can
    filter noise from real problems.
  - You want logs saved to files so you can review them later.
  - You want every module to share the SAME logger config.

Python's built-in `logging` module handles all of this.

How to use in any module:
    from src.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.warning("Missing values found: 12")
    logger.error("File not found: data/raw/online_retail.csv")
"""

import logging
import sys
from pathlib import Path
from src.config import settings


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    Args:
        name: Usually pass __name__ — Python automatically fills this with
              the current module name (e.g. 'src.profiling.profiler').

    Returns:
        A logging.Logger instance that writes to both console and a log file.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if get_logger is called multiple times
    if logger.handlers:
        return logger

    # Set the minimum severity level from config
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)

    # ── Format ────────────────────────────────────────────────────────────
    # Example output:
    # 2025-08-25 14:30:01 | INFO     | src.profiling.profiler | Loaded 541909 rows
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console handler — prints to terminal ──────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ── File handler — writes to logs/datatrust.log ───────────────────────
    settings.ensure_directories()
    log_file = Path(settings.LOGS_PATH) / "datatrust.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
