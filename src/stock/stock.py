from pathlib import Path
import sys
# Add the parent directory of the project to sys.path to allow absolute imports
sys.path.insert(0, Path(__file__).parents[2].absolute().__str__())

# Standard packages
import pandas as pd
from core.config_loader import ConfigLoader  # Load configuration from YAML
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import numpy as np

# Custom packages
from utils.logger import get_logger         # Custom logger setup
from utils.utils import import_masterdata   # Helper to load master data
from utils.utils import send_mail           # Helper to send email notifications
from utils.constant import PATH_DATA

# Initialize logger
logger = get_logger("stock")

# Define project folder structure
path_data = PATH_DATA
path_data_stock = path_data / "stock"
path_data_stock_input = path_data_stock / "input"
path_data_stock_history = path_data_stock / "history"
path_data_stock_current = path_data_stock_input / "current"
path_data_stock_past = path_data_stock_input / "past"


def load_n_concat_stock(
        path_data_stock: Path,
        path_data_stock_current: Path,
        path_data_stock_past: Path,
        path_data_stock_history: Path) -> pd.DataFrame:
    
    """
    Load the the current and past stock data from specified file paths,
    concatenate them into a single DataFrame, and return the combined data.

    Parameters:
    - path_data_stock_input (Path): Path to the stock data file.
    - path_data_stock_history (Path): Path to save historical stock data.

    Returns:
    - pd.DataFrame: Corrected stock data with adjusted YearWeek.
    """

    def load_data(path_data_stock_current: Path,
                  file_name: str) -> pd.DataFrame:
        df_stock = pd.read_csv(path_data_stock_current / file_name, 
                                    sep="\t",
                                    encoding="utf-8",
                                    decimal='.',
                                    dtype={
                                        "UPC": str,
                                        "Datest WHS": str,
                                        "Stock Category (6 digit) Code": str
                                    })
        return df_stock

    # Load the stock data
    try:
        df_stock_current = load_data(path_data_stock_current, file_name="stock_current.txt")

        logger.info(f"Loaded stock_current data with {len(df_stock_current)} rows from {path_data_stock_current / 'stock_current.txt'}")
    except FileNotFoundError as e:
        msg = (f"File 'stock_current.txt' not found in {path_data_stock_current}. "
               f"It may have already been processed and moved to the history folder. Othrwise, something went wrong during file creation. Refer to Gruppo NPI Demand Planning.")
        logger.error(msg)
        raise FileNotFoundError(msg) from e
    

    try:
        df_stock_past = load_data(path_data_stock_past,  file_name="stock_past.txt")
                                    
        logger.info(f"Loaded stock_past with {len(df_stock_past)} rows from {path_data_stock_past / 'stock_past.txt'}")
    except FileNotFoundError as e:
        msg = (f"File 'stock_past.txt' not found in {path_data_stock_past}. "
               f"You have to download it from Business Object first.")
        logger.error(msg)
        raise FileNotFoundError(msg) from e
    
    logger.info("Aligning YearWeek")
    # Align YearWeek format
    df_stock_past['Year'] = df_stock_past['Year'] + 1
    df_stock_past['Year'] = df_stock_past['Year'].astype(int).astype(str).str.zfill(4)  # Ensure Year is 4 digits
    df_stock_past['Year Week'] = df_stock_past['Year Week'] + 100
    df_stock_past['Year Week'] = df_stock_past['Year Week'].astype(int).astype(str).str.zfill(6)  # Ensure Year Week is 6 digits
    
    df_stock = pd.concat([df_stock_current, df_stock_past], ignore_index=True)
    logger.info(f"Concatenated stock_current and stock_past data, total rows: {len(df_stock)}")


    df_stock_current.to_csv(path_data_stock_history / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_stock_current.txt", index=False, sep="\t")
    logger.info(f"Saved historical stock_current data to {path_data_stock_history / f'{datetime.now().strftime('%Y%m%d_%H%M%S')}_stock_current.txt'}")


    df_stock.to_csv(path_data_stock / "stocks.csv", index=False, sep=";")
    logger.info(f"Saved stock data to {path_data_stock / 'stocks.csv'}")
    
    Path.unlink(path_data_stock_current / "stock_current.txt")
    logger.info("Original file 'stock_current.txt' removed from 'input/current' folder.")
    return df_stock

if __name__ == "__main__":
    
    try:
        conf = ConfigLoader(Path(__file__).parents[2] / "config" / "config.yaml")
        df_stock = load_n_concat_stock(
            path_data_stock=path_data_stock,
            path_data_stock_current=path_data_stock_current,
            path_data_stock_past=path_data_stock_past,
            path_data_stock_history=path_data_stock_history)
        
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="stock Script Completed Successfully",
            text=f"stock data has been successfully processed.",
        )

    except Exception as e:
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Error in stock Script",
            text=f"An error occurred while processing the stock data: {e}.",
        )
        logger.error(f"An error occurred: {e}")
        sys.exit(1)