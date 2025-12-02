# E2E Project 

## How to run (first time for a new report)

1) Open the file `config/config.yaml` and set:
   - `release`: .This is the current release you want to monitor. Use the format 'YYYY NN'.
   - `release_ly`: .This is the past release you want to use as "last year" reference. Use the format 'YYYY NN'.
   - `npi_sql_server` (change only if you are sure): change the `server` and the `database` of the SQL Server for the "Apollo" data (charged data).

2) Run the file `e2e_update.bat`. It should be already scheduled inside *Task Scheduler* within the server `10.200.112.107` (*please refer to Roleof or someone inside the Data Science Community if you cannot open the server*).

## How to run (after first time)
Run the file `e2e_update.bat`. Usually, once per week. It should be already scheduled inside *Task Scheduler* within the server `10.200.112.107` (*please refer to Roleof or someone inside the Data Science Community if you cannot open the server*).

## Files
### ⚙️ config.yaml
The `config/config.yaml` is essential to run correctly all the scripts and it should be updated every time is need. For instnace, it has to be updated:
- When you create a new report and you have to change the releases
- When The emails from and to whom you want to send the email of the status of the scripts.
- When the SQL Server of the Apollo system changes

### 🏃‍♀️ e2e_update.bat
The file `src/e2e_update.bat` runs each python script (once for each type of data) by exploiting the conda environment `C:\Users\mottad\AppData\Local\miniconda3\envs\e2e_env`. Notice that the environement is inside `mottad`. Hence if you are not me (lol) this file as well as all the python scripts couldn't run properly.




## 📂 Directory Tree

```text
script/
├── config/               # Configuration management
│   └── config.yaml       # Main configuration file (release, server, etc.)
├── core/                 # Core components required for script initialization
│   └── config_loader.py  # Module responsible for loading and validating config.yaml
├── query/                # Directory containing SQL queries or data definition files
├── src/                  # Source Code: Business logic divided by domain
│   ├── backorder/        # Logic related to backordered items
│   ├── charged/          # Logic related to charges/costs processing
│   ├── otif/             # OTIF (On Time In Full) metrics calculation and management
│   ├── sales/            # Sales data analysis and processing
│   ├── stock/            # Inventory and stock management
│   └── e2e_update.bat    # Batch script for Windows updates or process execution
├── utils/                # Shared support libraries and helpers
│   ├── constant.py       # Global project constants definitions like OS
│   ├── logger.py         # Module for logging configuration and handling
│   └── utils.py          # Generic reusable utility functions like "send_email"
├── .gitignore            # List of files and folders excluded from Git versioning
├── e2e.code-workspace    # VS Code workspace configuration settings
├── environment.yaml      # Environment dependency file (e.g., for Conda)
└── README.md             # General project documentation