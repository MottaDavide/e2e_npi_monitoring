from pathlib import Path
import sys
# Add the parent directory of the project to sys.path to allow absolute imports
sys.path.insert(0, Path(__file__).parents[2].absolute().__str__())

# Standard packages
import pandas as pd
from core.config_loader import ConfigLoader  # Load configuration from YAML
from datetime import datetime

# Custom packages
from utils.logger import get_logger         # Custom logger setup
from utils.utils import import_masterdata   # Helper to load master data
from utils.utils import send_mail           # Helper to send email notifications
from utils.constant import PATH_DATA

# Initialize logger
logger = get_logger("otif_append")

# Define project folder structure

path_data = PATH_DATA
path_data = Path(__file__).parents[3] / "data"
path_data_otif =path_data / "otif"
path_data_otif_history = path_data_otif / "history"
path_data_otif_actual = path_data_otif / "actual"



from datetime import datetime



def load_n_filter_n_backup_otif_actual(
    path_data_otif_actual: Path,
    path_data_otif_history: Path,
    release: str | None = None
) -> pd.DataFrame:
    """
    Load the OTIF actual data file, filter it by a given release value,
    and save a backup of the filtered file with a timestamp in the history folder.

    Parameters:
    - path_data_otif_actual (Path): Path to the folder containing '00_OTIF_Reclass.txt'.
    - path_data_otif_history (Path): Path to the folder where filtered backups should be stored.
    - release (str | None): Optional release value to filter the data on.

    Returns:
    - pd.DataFrame: Filtered OTIF actual data.
    """

    # Try to load the OTIF actual data file
    try:
        df_otif_actual = pd.read_csv(
            path_data_otif_actual / "00_OTIF_Reclass.txt",
            sep=';',
            encoding='cp1252',
            dtype={
                "Yyear": str,
                "Mmonth": str,
                "Wweek": str,
                "SubCode": str,
                "OrderSpecification": str,
                "KeyAccount": str,
                'CP_cluster': str,
                'DS_cluster': str,
                'HUBDS': str,
                "ASSORTMENT": str,
                "OTIF Num Shipped Qty Net": float,
                "OTIF Den ToBeShpped Qty Net": float
            }
        )
        logger.info(f"Loaded OTIF actual data with {len(df_otif_actual)} rows from {path_data_otif_actual / '00_OTIF_Reclass.txt'}")
    except FileNotFoundError as e:
        msg = (f"File '00_OTIF_Reclass.txt' not found in {path_data_otif_actual}. "
               f"It may have already been processed and moved to the history folder. Othrwise, something went wrong during file creation. Refer to Gruppo Wholesale Forecasting.")
        logger.error(msg)
        raise FileNotFoundError(msg) from e

    # Filter the DataFrame by the 'Release' column if provided
    if release is not None:
        df_otif_actual = df_otif_actual[df_otif_actual["Release"] == release]
        logger.info(f"Filtered OTIF actual data by Release = '{release}', rows remaining: {len(df_otif_actual)}")

    # Save a backup of the filtered DataFrame with a timestamp
    backup_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_00_OTIF_Reclass.txt"
    backup_path = path_data_otif_history / backup_filename
    df_otif_actual.to_csv(backup_path, sep=';', encoding='cp1252', index=False)
    logger.info(f"Backup saved to {backup_path}")



    return df_otif_actual

def clean_otif_actual(df_otif_actual: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the OTIF actual DataFrame by renaming columns to more user-friendly names. Moreiver it creates the 'otif_year', 'otif_month', 'otif_quarter','otif_week' fields by adding 1 week to the 'week' field.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing OTIF actual data.

    Returns:
    - pd.DataFrame: The cleaned DataFrame with renamed columns.
    """
    df_otif_actual = df_otif_actual.rename(columns={
        "OTIF Num Shipped Qty Net": "shipped_qty_net",
        "OTIF Den ToBeShpped Qty Net": "to_be_shipped_qty_net",
        "Yyear": "year",
        "Mmonth": "month",
        "Wweek": "week",
        "Qquarter": "quarter",
        "Grid": "grid",
        "Style": "model",
        "BusinessUnit": "business_unit",
        "OrderSpecification": "order_specification",
        "OrderType": "order_type",
        "KeyAccount": "key_account",
        "Region": "region"
    })

    df_otif_actual = df_otif_actual[[
        "model", "grid", "year", "month", "week", "quarter", "business_unit",
        "order_specification", "order_type", "key_account", "region",
        "shipped_qty_net", "to_be_shipped_qty_net"
    ]]

    logger.info("OTIF actual data cleaned and columns renamed.")

    # Create 'otif_year', 'otif_month', 'otif_quarter', 'otif_week' fields by adding 1 week to the 'week' field

    df_otif_actual['date'] = pd.to_datetime(
        df_otif_actual['year'].astype(str) + 
        df_otif_actual['week'].astype(str) + "1",
        format='%G%V%u'
    )


    df_otif_actual['otif_date'] = df_otif_actual['date'] + pd.Timedelta(days=7) # Adding 1 week to the date
    df_otif_actual['otif_year'] = df_otif_actual['otif_date'].dt.isocalendar().year
    df_otif_actual['otif_month'] = df_otif_actual['otif_date'].dt.month
    df_otif_actual['otif_quarter'] = 'Q' +  df_otif_actual['otif_date'].dt.quarter.astype(str)
    df_otif_actual['otif_week'] = df_otif_actual['otif_date'].dt.isocalendar().week

    df_otif_actual = df_otif_actual.drop(columns=['date', 'otif_date'])
    logger.info("Added 'otif_year', 'otif_month', 'otif_quarter', 'otif_week' fields to the OTIF.")

    return df_otif_actual

def append_otif_actual_to_main_file(df_otif_actual: pd.DataFrame, 
                                    path_data_otif: Path, 
                                    file_name: str = "otif.csv") -> None:
    """
    Append the cleaned OTIF actual data to the main csv file.
    If the file (main) does not exist, it will be created.
    Moreover, recreate the UPC column from master_data file.

    Parameters:
    - df_otif_actual (pd.DataFrame): The cleaned and filtered actual OTIF data.
    - path_data_otif (Path): The path to the folder containing the main csv file.
    - file_name (str): The name of the csv file to append to.
    """

    csv_path = path_data_otif / file_name #otif.csv

    if not csv_path.exists():
        df_otif_actual.to_csv(csv_path, index=False, sep=";")
        logger.info(f"Created new CSV file with {len(df_otif_actual)} rows at {csv_path}")
        return

    # Load existing CSV file
    df_existing = pd.read_csv(csv_path, 
                              dtype={"shipped_qty_net": float,
                                     "to_be_shipped_qty_net": float,
                                     'year': str,
                                     'month': str,
                                     'week': str,
                                     'quarter': str,
                                     'UPC': str},
                              sep=";")
    logger.info(f"Loaded existing CSV file with {len(df_existing)} rows.")

 
    df_to_append = df_otif_actual.merge(df_existing.drop_duplicates(), how='left', indicator=True)
    df_to_append = df_to_append[df_to_append['_merge'] == 'left_only'].drop(columns=['_merge'])


    if df_to_append.empty:
        logger.info("No new rows to append. File is up-to-date.")
        return
    
 
 
    df_updated = pd.concat([df_existing, df_to_append], ignore_index=True)
    logger.info(f"Appended {len(df_to_append)}  new rows to OTIF file.")
   
    # Rebuild UPC using master data
    df_masterdata = import_masterdata(
        path_data / "master_data" / "master_data.xlsx",
        dtype=object,
        usecols=["Model", "Size", "Color", "UPC"]
    )
    df_masterdata['sku'] = df_masterdata['Model'] + (df_masterdata['Color'] + " " * 10).str[:6] + df_masterdata['Size'] # Create SKU field as Model + Grid
    df_updated['sku'] = df_updated['model'] + df_updated['grid'] # Create SKU field as Model + Grid

    df_updated = df_updated[[col for col in df_updated.columns if col not in ['UPC']]].merge(df_masterdata[['sku', 'UPC']], on='sku', how='inner')
    df_updated = df_updated.drop(columns=['sku'])
    logger.info("Updated 'UPC' column using master data.")

 

    
    df_updated.to_csv(csv_path, index=False, sep=";")
    logger.info(f"Saved OTIF data to CSV file at {csv_path}.")

    Path.unlink(path_data_otif_actual / "00_OTIF_Reclass.txt")
    logger.info("Original file '00_OTIF_Reclass.txt' removed from 'actual' folder.")



if __name__ == "__main__":
    
    try:
        conf = ConfigLoader(Path(__file__).parents[2] / "config" / "config.yaml")
        release = conf.get("release")

        df_otif_actual = load_n_filter_n_backup_otif_actual(path_data_otif_actual, path_data_otif_history, release)
        df_otif_actual = clean_otif_actual(df_otif_actual)
        append_otif_actual_to_main_file(df_otif_actual, path_data_otif)

        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="OTIF Append Script Completed Successfully",
            text=f"OTIF data has been successfully processed and appended to the main file. Release: {release}",
        )
    except Exception as e:
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Error in OTIF Append Script",
            text=f"An error occurred while processing the OTIF data: {e}",
        )
        logger.error(f"An error occurred: {e}")
        sys.exit(1)


    
