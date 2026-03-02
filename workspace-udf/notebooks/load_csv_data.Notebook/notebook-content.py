# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Load CSV Data from GitHub to Fabric Lakehouse
# 
# This notebook downloads all CSV files from the GitHub repository
# [unified-data-foundation-with-fabric-solution-accelerator](https://github.com/microsoft/unified-data-foundation-with-fabric-solution-accelerator)
# (branch: `agh-workbook-sanitization`, path: `infra/data`) and uploads them to a Fabric Lakehouse
# in the current workspace, preserving the original folder structure.
# 
# **What this notebook does:**
# 1. Uses `GitHubDownloader` from `fabric-launcher` to download `infra/data/` from the GitHub repo into a local temp directory
# 2. Uses `LakehouseFileManager` from `fabric-launcher` to copy all CSV files from the temp directory to the target Lakehouse, mirroring the source folder hierarchy
# 
# **Expected source structure (preserved in Lakehouse):**
# ```
# Files/data/
# ├── samples_databricks/sales/
# ├── samples_fabric/finance/
# ├── samples_fabric/sales/
# ├── samples_fabric/shared/
# └── samples_snowflake/sales/
# ```

# MARKDOWN ********************

# ## 1. Install & Import Libraries

# CELL ********************

%pip install fabric-launcher --quiet
import notebookutils
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Configure Settings
# 
# Set the GitHub source and the target Lakehouse name.

# CELL ********************

from fabric_launcher import GitHubDownloader, LakehouseFileManager

# ── GitHub source settings ────────────────────────────────────────────────────
GITHUB_OWNER     = "microsoft"
GITHUB_REPO      = "unified-data-foundation-with-fabric-solution-accelerator"
GITHUB_BRANCH    = "main"
GITHUB_DATA_PATH = "infra/data"   # Folder inside the repo to download

# ── Fabric Lakehouse target settings ─────────────────────────────────────────
LAKEHOUSE_NAME = "maag_bronze"            # Lakehouse in the current workspace
TARGET_FOLDER  = "/"                   # Root folder under Lakehouse Files/

# ── Temporary local directory ─────────────────────────────────────────────────
LOCAL_TEMP_DIR = "/tmp/udf_data"          # Where the repo folder is extracted

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Download CSV Files from GitHub
# 
# `GitHubDownloader` downloads the repository as a ZIP and extracts only the `infra/data/` subfolder to a local temp directory, preserving the full folder structure.

# CELL ********************

downloader = GitHubDownloader(
    repo_owner=GITHUB_OWNER,
    repo_name=GITHUB_REPO,
    branch=GITHUB_BRANCH,
)

# Download the repo ZIP and extract only infra/data/ → LOCAL_TEMP_DIR
# remove_folder_prefix strips the "infra/data/" prefix so the local layout becomes:
#   LOCAL_TEMP_DIR/samples_fabric/sales/Order_Samples_Fabric.csv  etc.
downloader.download_and_extract_folder(
    extract_to=LOCAL_TEMP_DIR,
    folder_to_extract=GITHUB_DATA_PATH,
    remove_folder_prefix=GITHUB_DATA_PATH + "/",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Upload CSV Files to Lakehouse
# 
# `LakehouseFileManager` mounts the Lakehouse via `notebookutils`, then recursively walks the local temp directory and copies every CSV file to `Files/<TARGET_FOLDER>/`, preserving the subfolder structure.

# CELL ********************

file_manager = LakehouseFileManager(notebookutils)

file_manager.copy_folder_to_lakehouse(
    lakehouse_name=LAKEHOUSE_NAME,
    source_folder=LOCAL_TEMP_DIR,
    target_folder=TARGET_FOLDER,
    file_patterns=["*.csv"],   # Only upload CSV files
    recursive=True,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
