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
logger = get_logger("charged")

# Define project folder structure
path_data = Path(__file__).parents[3] / "data"
path_query = Path(__file__).parents[2] / "query"
path_data_charged =path_data / "charged"


def create_weekly_charged_qty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a weekly charged quantity DataFrame from the input DataFrame. The steps are:
    0. Order the Dataframe by 'UPC' and 'UpdateYearWeekKey' in ascending order.
    1. Group the data by 'UPC'
    2. Differentiate the 'TotalChargedQuantity' by 1 to get weekly charged quantities.

    Parameters:
    - df (pd.DataFrame): Input DataFrame containing charged data.

    Returns:
    - pd.DataFrame: DataFrame with weekly charged quantities.
    """

    df = df.copy()
    
    df = df.sort_values(by=["UPC", "UpdateYearWeekKey"], ascending=True)

    df["WeeklyChargedQuantity"] = df.groupby(by=["UPC"])["TotalChargedQuantity"].diff(1).fillna(0)

    return df


def load_n_filter_charged(
        server: str, 
        database: str,
        query: str,
        release:str,
        release_ly: str
) -> pd.DataFrame:
    """
    Load the Charged data from the SQL Server and filter it by  specific releases. The steps are:
    1. Connect to the SQL Server using the provided server and database.
    2. Execute the provided SQL query, replacing the :release placeholder with the actual release value.
    3. Importing masterdata file.
    4. Perform inner join with masterdata to keep only relevant records.
    5. Return the filtered Charged data as a DataFrame.

    Parameters:
    - server (str): SQL Server name.
    - database (str): Database name.
    - query (str): SQL query to execute.
    - release (str): Release value to filter the data.
    - release_ly (str): Last year release value to filter the data.

    Returns:
    - pd.DataFrame: Filtered Charged data.
    """
    
    # Connect to the SQL Server
    engine = create_engine(
        f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&Encrypt=yes&TrustServerCertificate=yes"
    )

    release = release.replace(" ", "-") # Replace spaces with hyphens for consistency
    release_ly = release_ly.replace(" ", "-") # Replace spaces with hyphens for consistency

    # Read the query and replace the :release placeholder
    query = query.replace(":release", f"('{release}', '{release_ly}')").replace('Â', ' ')

    logger.info(f"Query:, {query}")

    # Execute the query and load the data into a DataFrame
    logger.info(f"Executing charged query for release: {release}")
    df = pd.read_sql(query, engine)

    # import masterdata
    logger.info("Importing masterdata")
    df_masterdata = import_masterdata(
        path_data / "master_data" / "master_data.xlsx",
        dtype=str,
        usecols=["Model", "Size", "Color", "UPC"]
    )
    df_masterdata['sku'] = df_masterdata['Model'] + (df_masterdata['Color'] + " " * 10).str[:6] + df_masterdata['Size'] # Create SKU field as Model + Grid
    df['sku'] = df['model'] + df['grid'] # Create SKU field as Model + Grid

    logger.info("Performing inner join with masterdata to keep only relevant records")
    # Perform inner join with masterdata to keep only relevant records
    df = df[[col for col in df.columns if col not in ['UPC']]].merge(df_masterdata[['sku', 'UPC']], on='sku', how='inner')

    logger.info("Aligning YearWeek")
    # Align YearWeek format
    df['UpdateYearWeekKey'] = np.where(df['ClusterKey'] == release_ly, df['UpdateYearWeekKey'] + 100, df['UpdateYearWeekKey'])
    df['UpdateYearWeekKey'] = df['UpdateYearWeekKey'].astype(int).astype(str).str.zfill(6)  # Ensure YearWeek is 6 digits

    logger.info("Creating weekly charged quantity")
    df = create_weekly_charged_qty(df)
   
    return df


if __name__ == "__main__":
    try:
        conf = ConfigLoader(Path(__file__).parents[2] / "config" / "config.yaml")
        release = conf.get("release")
        release_ly = conf.get("release_ly")
        server = conf.get("npi_sql_server")["server"]
        database = conf.get("npi_sql_server")["database"]

        query_path = path_query / "charged_v1.sql"

        with open(query_path, 'r') as file:
            query = file.read()
        
        df_charged = load_n_filter_charged(server, database, query, release, release_ly)

        # Save the DataFrame to a CSV file
        csv_path = path_data_charged / f"charged.csv"
        df_charged.to_csv(csv_path, index=False, sep = ';')
        logger.info(f"Saved Charged data to CSV file at {csv_path}")

        
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Charged Script Completed Successfully",
            text=f"Charged data has been successfully processed. Release: {release}",
        )
        
    except Exception as e:
        
        send_mail(
            mail_sender=conf.get("mail_sender"),
            mail_recipients=conf.get("mail_recipients"),
            subject="Error in Charged Script",
            text=f"An error occurred while processing the Charged data: {e}",
        )
        
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

