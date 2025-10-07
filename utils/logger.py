import logging

import logging
import sys
from pathlib import Path
from datetime import datetime

# === Setup logger ===
def get_logger(logger_name: str = "global") -> logging.Logger:
    """
    Set up and return a logger instance that logs both to file and stdout.

    Parameters:
    - logger_name (str): Name of the logger to be retrieved or created.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    log_dir = Path(__file__).resolve().parents[2] / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_log_file.log"

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    if not logger.handlers:
        # File handler
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)

        # Stream handler
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        sh.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(sh)

    return logger