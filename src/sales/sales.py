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
logger = get_logger("sales")

# Define project folder structure
path_data = PATH_DATA
path_data_sales = path_data / "sales"
path_data_sales_input = path_data_sales / "input"
path_data_sales_history = path_data_sales / "history"
path_data_sales_current = path_data_sales_input / "current"
path_data_sales_past = path_data_sales_input / "past"


def load_n_concat_sales(
        path_data_sales: Path,
        path_data_sales_current: Path,
        path_data_sales_past: Path,
        path_data_sales_history: Path) -> pd.DataFrame:
    
    """
    Load the the current and past sales data from specified file paths,
    concatenate them into a single DataFrame, and return the combined data.

    Parameters:
    - path_data_sales_input (Path): Path to the sales data file.
    - path_data_sales_history (Path): Path to save historical sales data.

    Returns:
    - pd.DataFrame: Corrected sales data with adjusted YearWeek.
    """

    def load_data(path_data_sales_current: Path,
                  file_name: str) -> pd.DataFrame:
        df_sales = pd.read_csv(path_data_sales_current / file_name, 
                                    sep="\t",
                                    encoding="utf-8",
                                    decimal='.',
                                    dtype={
                                        "UPC": str,
                                        "Datest WHS": str,
                                    })
        return df_sales

    # Load the sales data
    try:
        df_sales_current = load_data(path_data_sales_current, file_name="sales_&_shipping_current.txt")

        logger.info(f"Loaded sales_current data with {len(df_sales_current)} rows from {path_data_sales_current / 'sales_&_shipping_current.txt'}")
    except FileNotFoundError as e:
        msg = (f"File 'sales_&_shipping_current.txt' not found in {path_data_sales_current}. "
               f"It may have already been processed and moved to the history folder. Othrwise, something went wrong during file creation. Refer to Gruppo NPI Demand Planning.")
        logger.error(msg)
        raise FileNotFoundError(msg) from e
    

    try:
        df_sales_past = load_data(path_data_sales_past,  file_name="sales_&_shipping_past.txt")
                                    
        logger.info(f"Loaded sales_past with {len(df_sales_past)} rows from {path_data_sales_past / 'sales_&_shipping_past.txt'}")
    except FileNotFoundError as e:
        msg = (f"File 'sales_&_shipping_past.txt' not found in {path_data_sales_past}. "
               f"You have to download it from Business Object first.")
        logger.error(msg)
        raise FileNotFoundError(msg) from e
    
    df_sales = pd.concat([df_sales_current, df_sales_past], ignore_index=True)
    logger.info(f"Concatenated sales_current and sales_past data, total rows: {len(df_sales)}")


    df_sales_current.to_csv(path_data_sales_history / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_sales_&_shipping_current.txt", index=False, sep="\t")
    logger.info(f"Saved historical sales_current data to {path_data_sales_history / f'{datetime.now().strftime('%Y%m%d_%H%M%S')}_sales_&_shipping_current.txt'}")


    df_sales.to_csv(path_data_sales / "sales_&_shippings.csv", index=False, sep=";")
    logger.info(f"Saved sales data to {path_data_sales / 'sales_&_shippings.csv'}")
    
    Path.unlink(path_data_sales_current / "sales_&_shipping_current.txt")
    logger.info("Original file 'sales_&_shipping_current.txt' removed from 'input/current' folder.")
    return df_sales




if __name__ == "__main__":
    
    try:
        conf = ConfigLoader(Path(__file__).parents[2] / "config" / "config.yaml")
        df_sales = load_n_concat_sales(
            path_data_sales=path_data_sales,
            path_data_sales_current=path_data_sales_current,
            path_data_sales_past=path_data_sales_past,
            path_data_sales_history=path_data_sales_history)
        
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Sales Script Completed Successfully",
            text=f"Sales data has been successfully processed.",
        )

    except Exception as e:
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Error in Sales Script",
            text=f"An error occurred while processing the Sales data: {e}.",
        )
        logger.error(f"An error occurred: {e}")
        sys.exit(1)