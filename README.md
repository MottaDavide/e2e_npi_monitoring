# OTIF Data Pipeline

This Python project automates the processing and aggregation of OTIF (On Time In Full) data extracted weekly from an Access database, and appends it to a historical CSV (`otif.csv`).

## 📁 Project Structure

```text
E2E (WORKSPACE)
│
├── config/
│   └── config.yaml              # Contains general settings like 'release'
│
├── core/
│   └── config_loader.py         # Utility to load configuration from YAML
│
├── src/otif/
│   ├── otif_append.py           # Main script to process and append OTIF data
│   ├── otif_append.bat          # Optional batch file to trigger the script
│
├── utils/
│   ├── constant.py              # Constants used across the project
│   ├── logger.py                # Custom logger configuration
│   └── utils.py                 # Generic utilities (if needed)
│
└── data/otif/                   # Not shown, but assumed structure
    ├── actual/                  # Holds latest weekly extract
    ├── history/                 # Stores timestamped backup files
    └── otif.csv                 # The main aggregated OTIF dataset
```
## 🔍 Objective

This pipeline:
1. Loads the weekly OTIF data from a `.txt` export (coming from an MS Access query).
2. Optionally filters the data by a `Release` value defined in `config.yaml`.
3. Saves a timestamped backup in the `history/` folder.
4. Cleans and standardizes the data schema.
5. Appends only new rows to the central `otif.csv`.

## 📥 Input Source

The weekly OTIF data originates from an Access Database:

`\luxapplp04\share\Gruppo_Distribution_Planning\00_Data_Bases\OTIF\Source\OTIF_Input - PRJ OTIF 311.accdb`

Export automatically the query named:

`00_OTIF_Reclass`

as:

`00_OTIF_Reclass.txt`

and save it in:

`data/otif/actual/`

## ⚙️ How It Works

### Step-by-Step Logic

1. **Load Config:**
   - Reads the `release` filter from `config.yaml`.

2. **Load & Filter OTIF Data:**
   - Loads `00_OTIF_Reclass.txt` from `actual/`.
   - Applies filtering by `Release` if specified.
   - Logs the data size and key events.

3. **Backup:**
   - Saves a timestamped copy in `history/`.

4. **Clean Data:**
   - Renames columns to standardized snake_case.
   - Keeps only relevant fields for analysis.
   - Create the 'otif_year', 'otif_month', 'otif_quarter','otif_week' fields by adding 1 week to the 'week' field.

5. **Append Logic:**
   - Loads existing `otif.csv` if available.
   - Appends only new rows (avoids duplicates).
   - Recreate the UPC column from data/master_data/master_data.xlsx file.
   - Saves updated file.

6. **Remove Original Input:**
   - Deletes the processed `.txt` to avoid reprocessing.

7. **Send an email:**
   - Send and email to inform the user about the status of the pipeline.

## ▶️ How to Run
### 0. Install Miniconda and VS Code

Skip this point if you already have Miniconda ora Anaconda and VS Code installed.

Otherwise, follow the links
-  [Install Miniconda](https://www.anaconda.com/docs/getting-started/miniconda/install)

- [Install VS Code](https://code.visualstudio.com/docs/setup/windows)
### 1. Create the Conda Environment

From the terminal (macOS) or Anaconda Prompt (Windows), run:

```bash
conda env create -f environment.yaml
```

This will create a new Conda environment named `e2e_env` with all required dependencies.

### 2. Activate the Environment

Activate the environment using:

- **macOS/Linux**:
  ```bash
  conda activate e2e_env
  ```

- **Windows**:
  ```cmd
  conda activate e2e_env
  ```

### 4. Run the Script
- Open VS Code
- Open the script `otif-append.py`
- Run Current File in interactive Window![Run Current File in interactive Window](image.png)