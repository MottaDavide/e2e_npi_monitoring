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
logger = get_logger("backorder")

# Define project folder structure
path_data = PATH_DATA
path_data_backorder = path_data / "backorder"
path_data_backorder_input = path_data_backorder / "input"
path_data_backorder_history = path_data_backorder / "history"



import pandas as pd
from datetime import timedelta


def load_n_correct_backorder(
        path_data_backorder_input: Path,
        path_data_backorder_history: Path) -> pd.DataFrame:
    """
    Load the backorder data from a specified file path, identify YearWeeks with multiple load dates,
    and adjust the YearWeek for the oldest load date by moving it to the previous week.
    Evetually,

    Parameters:
    - path_data_backorder_input (Path): Path to the backorder data file.
    - path_data_backorder_history (Path): Path to save historical backorder data.

    Returns:
    - pd.DataFrame: Corrected backorder data with adjusted YearWeek.
    """

    # Load the backorder data
    try:
        df_backorder = pd.read_csv(path_data_backorder_input / "backorder.txt", 
                                    sep="\t",
                                    encoding="utf-8",
                                    thousands='.',
                                    dtype={
                                        "Year Week": str,
                                        "UPC Code": str,
                                        "Pipeline - DtLoad": str,
                                        "Pipeline - PurchaseFromSuppliers": float,
                                        "Pipeline - Unsatisfied Demand": float,
                                        "Pipeline - WIP": float,
                                        "TotalDemand": float,
                                        "Productive BO": float,
                                        "Logistic BO": float,
                                        "DatestSubsidiaryCode": str,
                                        "Direct Shipment BO": float,
                                        "ECom - RetailModel": float, 
                                        "To Be Shipped BO": float
                                        
                                    })

        logger.info(f"Loaded Backorder actual data with {len(df_backorder)} rows from {path_data_backorder_input / 'backorder.txt'}")
    except FileNotFoundError as e:
        msg = (f"File 'backorder.txt' not found in {path_data_backorder_input}. "
               f"It may have already been processed and moved to the history folder. Othrwise, something went wrong during file creation. Refer to Gruppo NPI Demand Planning.")
        logger.error(msg)
        raise FileNotFoundError(msg) from e
    # Convert the load date to datetime
    df_backorder["Pipeline - DtLoad"] = pd.to_datetime(df_backorder["Pipeline - DtLoad"], dayfirst=True)

    # Create a new column for adjusted YearWeek
    df_backorder["YearWeek_Adjusted"] = df_backorder["Year Week"]

    # Identify YearWeeks with more than one load date
    multiple_loads = (
        df_backorder.groupby("Year Week")["Pipeline - DtLoad"]
        .nunique()
        .loc[lambda x: x > 1]
        .index
    )

    if not multiple_loads.empty:
        logger.info(f"  Found YearWeeks with multiple load dates: {list(multiple_loads)}")
        # For each such YearWeek, move the oldest load date to the previous week
        for yw in multiple_loads:
            subset = df_backorder[df_backorder["Year Week"] == yw]
            oldest_date = subset["Pipeline - DtLoad"].min()

            # Compute the new YearWeek by subtracting 7 days
            new_date = oldest_date - timedelta(days=7)
            new_year = new_date.isocalendar().year
            new_week = new_date.isocalendar().week
            new_yw = f"{new_year}{new_week:02d}"

            # Apply correction only to rows with the oldest load date
            condition = (df_backorder["Year Week"] == yw) & (df_backorder["Pipeline - DtLoad"] == oldest_date)
            df_backorder.loc[condition, "YearWeek_Adjusted"] = new_yw

            logger.info(f"  Adjusted YearWeek for load date {oldest_date.date()} from {yw} to {new_yw}")


        logger.info("  Completed YearWeek adjustments for backorder data.")
        # Drop the original Year Week column and rename the adjusted one
        df_backorder = df_backorder.drop(columns=["Year Week"]).rename(columns={"YearWeek_Adjusted": "Year Week"})
    
    # Change datatypes for the last columns
    df_backorder.iloc[:,-10:] = df_backorder.iloc[:,-10:].astype(int)

    df_backorder.to_csv(path_data_backorder_history / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_backorder.txt", index=False, sep="\t")
    logger.info(f"Saved historical backorder data to {path_data_backorder_history / f'{datetime.now().strftime('%Y%m%d_%H%M%S')}_backorder.txt'}")
    
    df_backorder.to_csv(path_data_backorder / "backorder.csv", index=False)
    logger.info(f"Saved backorder data to {path_data_backorder / 'backorder.csv'}")

    Path.unlink(path_data_backorder_input / "backorder.txt")
    logger.info("Original file 'backorder.txt' removed from 'input' folder.")
    
    
    
    return df_backorder


if __name__ == "__main__":

    try:
        conf = ConfigLoader(Path(__file__).parents[2] / "config" / "config.yaml")
        df_backorder = load_n_correct_backorder(
            path_data_backorder_input=path_data_backorder_input,
            path_data_backorder_history=path_data_backorder_history)
        
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Backorder Script Completed Successfully",
            text=f"Backorder data has been successfully processed.",
        )

    except Exception as e:
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Error in Backorder Script",
            text=f"An error occurred while processing the Backorder data: {e}.",
        )
        logger.error(f"An error occurred: {e}")
        sys.exit(1)