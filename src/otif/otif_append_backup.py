from pathlib import Path
import sys
sys.path.insert(0, Path(__file__).parents[2].absolute().__str__())

import pandas as pd

from core.config_loader import ConfigLoader
from datetime import datetime
from utils.logger import get_logger
from utils.constant import PATH_DATA



logger = get_logger("otif_append")

path_data = PATH_DATA
path_data_otif = Path(__file__).parents[3] / "data" / "otif"
path_data_otif_history = path_data_otif / "history"
path_data_otif_actual = path_data_otif / "actual"

from pathlib import Path
import pandas as pd
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

    if release is not None:
        df_otif_actual = df_otif_actual[df_otif_actual["Release"] == release]
        logger.info(f"Filtered OTIF actual data by Release = '{release}', rows remaining: {len(df_otif_actual)}")

    backup_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_00_OTIF_Reclass.txt"
    backup_path = path_data_otif_history / backup_filename
    df_otif_actual.to_csv(backup_path, sep=';', encoding='cp1252', index=False)
    logger.info(f"Backup saved to {backup_path}")

    Path.unlink(path_data_otif_actual / "00_OTIF_Reclass.txt")
    logger.info("Original file removed from actual folder.")

    return df_otif_actual

def clean_otif_actual(df_otif_actual: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the OTIF actual DataFrame by renaming columns to more user-friendly names.

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
    return df_otif_actual

def append_otif_actual_to_main_file(df_otif_actual: pd.DataFrame, 
                                    path_data_otif: Path, 
                                    file_name: str = "otif.parquet") -> None:
    """
    Append the cleaned OTIF actual data to the main Parquet file.
    If the file does not exist, it will be created.

    Parameters:
    - df_otif_actual (pd.DataFrame): The cleaned and filtered actual OTIF data.
    - path_data_otif (Path): The path to the folder containing the main Parquet file.
    - file_name (str): The name of the Parquet file to append to.
    """
    parquet_path = path_data_otif / file_name

    if not parquet_path.exists():
        df_otif_actual.to_parquet(parquet_path, index=False)
        logger.info(f"Created new Parquet file with {len(df_otif_actual)} rows at {parquet_path}")
        return

    df_existing = pd.read_parquet(parquet_path)
    logger.info(f"Loaded existing Parquet file with {len(df_existing)} rows.")

    df_to_append = df_otif_actual.merge(df_existing.drop_duplicates(), how='left', indicator=True)
    df_to_append = df_to_append[df_to_append['_merge'] == 'left_only'].drop(columns=['_merge'])

    if df_to_append.empty:
        logger.info("No new rows to append. File is up-to-date.")
        return

    df_updated = pd.concat([df_existing, df_to_append], ignore_index=True)
    df_updated.to_parquet(parquet_path, index=False)
    logger.info(f"Appended {len(df_to_append)} new rows to Parquet file at {parquet_path}.")

if __name__ == "__main__":
    conf = ConfigLoader(Path(__file__).parents[2] / "config" / "config.yaml")
    release = conf.get("release")

    df_otif_actual = load_n_filter_n_backup_otif_actual(path_data_otif_actual, path_data_otif_history, release)
    df_otif_actual = clean_otif_actual(df_otif_actual)
    append_otif_actual_to_main_file(df_otif_actual, path_data_otif)