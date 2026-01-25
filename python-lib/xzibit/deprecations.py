"""
Lists of deprecated items for DSS v14+
"""

import pandas as pd


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
import pandas as pd
import os

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
    df = pd.read_csv(file_path, dtype=str)
    
    return df


def lookup_python_support(dss_version, python_version, df=load_python_support()):
    """TBD"""
    if df is None:
        return "unknown"

    # check if
    row = df[
        (df["DSS_Version"] == dss_version) & (df["Python_Version"] == python_version)
    ]

    if row.empty:
        return "unknown"

    return row.iloc[0]["Support_Status"]


df_python_support = load_python_support()
