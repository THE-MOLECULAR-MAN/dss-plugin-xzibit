"""
Lists of deprecated items for DSS v14+
"""

import pandas as pd
import os


# List of deprecated preprocessor names for DSS v14+
DEPRECATED_PREPROCESSORS = {
    "AnonymizerProcessor",
    "MemoryEquiJoiner",
    "MemoryEquiJoinerFuzzy",
    "UseRowAsHeader",
    "NearestNeighbourGeoJoiner",
}

DEPRECATED_PLUGIN_IDS = {
    "looker-query",
    "emr-clusters",
    "nlp-nlg-tasks",
    "tableau-export-v2",
    "project-deployer",
    "moderna-llm",
    "forecastio",
    "model-data-compliance",
    "oncrawl",
    "join-and-keep-unmatched",
    "openai-gpt-text-completion",
    "timeseries-forecast-gpu-cuda100",
    "instagram",
    "microsoft-adls",
    "time-series-forecast",
    "list-folder-contents",
    "dataiku-project-bundle-migration",
    "twitter-tools",
    "model-drift",
    "hipchat",
    "hubspot",
    "rules-generation",
    "feature-generation-selection-GA",
    "deeplearning-image-gpu",
    "deeplearning-image",
    "h2o",
    "crowlingo-nlp",
    "timeseries-forecast",
    "natif-idp",
    "thread",
    "ovh-logs-import",
    "anonymizer",
    "advisor",
    "wunderground",
    "microsoft-power-bi",
    "meaningcloud",
    "snowflake",
    "deeplearning-image-cpu",
    "model-lightgbm",
    "azure-ad-sync",
    "nlp-language-detection",
    "namr-store",
    "events-aggregator",
}

DSS_BUILT_IN_PLUGIN_IDS = [
    "default-samples",
    "builtin-macros",
    "code-studio-blocks",
    "colorbrewer-palettes",
    "k8s-metrics-utils",
    "local-r-dev-setup",
    "project-standards",
]


def load_dss_version_support(file_path: str) -> pd.DataFrame:
    """
    Loads the DSS Version Python Support CSV into a pandas DataFrame.
    Enforces all columns to be read as strings to preserve version formatting
    (e.g., preventing '3.10' from becoming float 3.1).

    Args:
        file_path (str): The local path to the CSV file.

    Returns:
        pd.DataFrame: The loaded data with all values as strings.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file at {file_path} was not found.")

    # Using dtype=str ensures all data is read as strings
    return pd.read_csv(file_path, dtype=str)


def lookup_python_support(
    dss_version: str, python_version: str, df_dss_python: pd.DataFrame
) -> str:
    """
    Looks up the support status for a specific Python version in a given DSS version.

    Args:
        dss_version (str): The DSS Major Version (e.g., "11", "14").
        python_version (str): The Python version (e.g., "2.7", "3.9").
        support_df (pd.DataFrame, optional): The loaded support DataFrame.
                                             If None, loads from 'DSS_version_python_support.csv'.

    Returns:
        str: The support status (e.g., "supported", "partial", "deprecated").
             Returns "DSS Version Not Found" or "Python Version Not Found" if inputs do not match.
    """

    # 2. Input Validation (Sanitization)
    # Ensure inputs are strings and strip whitespace to prevent matching errors
    dss_ver = str(dss_version).strip()
    py_ver = str(python_version).strip()

    # 3. Locate the Row (DSS Version)
    # matching the 'DSS_Major_Version' column
    row = df_dss_python[df_dss_python["DSS_Major_Version"] == dss_ver]

    if row.empty:
        return f"DSS Version '{dss_ver}' Not Found"

    # 4. Locate the Column (Python Version) and Return Value
    if py_ver in df_dss_python.columns:
        # iloc[0] takes the first match (should be unique)
        return row.iloc[0][py_ver]
    else:
        return "Unknown"


# Example Usage
if __name__ == "__main__":
    # Option 1: Passing the dataframe (Efficient for multiple lookups)
    # Assuming load_dss_version_support is defined or we load manually
    print("Loading DSS version Python support data...")
    df_support_lookup = load_dss_version_support(
        "/Users/tim.honker@dataiku.com/Downloads/DSS_version_python_support.csv"
    )

    status = lookup_python_support("11", "2.7", df_support_lookup)
    print(f"DSS 11 with Python 2.7: {status}")  # Output: partial

    # Option 2: Standalone call (Loads file automatically)
    status_14 = lookup_python_support("14", "3.12", df_support_lookup)
    print(f"DSS 14 with Python 3.12: {status_14}")  # Output: supported
