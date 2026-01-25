"""
Lists of deprecated items for DSS v14+
"""

import os
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


def load_local_csv_as_dataframe(filename: str):
    """
    Loads the DSS Version Python Support CSV into a pandas DataFrame.
    Enforces all columns to be read as strings to preserve version formatting
    (e.g., preventing '3.10' from becoming float 3.1).

    Args:
        file_path (str): The local path to the CSV file.

    Returns:
        pd.DataFrame: The loaded data with all values as strings.
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # Fallback for interactive environments (like Jupyter) where __file__ is not defined
        script_dir = os.getcwd()

    file_path = os.path.join(script_dir, filename)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file at {file_path} was not found.")

    # Using dtype=str ensures all data is read as strings
    return pd.read_csv(file_path, dtype=str)


def lookup_recipe_deprecation_status(recipe_type, df_dss_recipes):

    # 2. Input Validation (Sanitization)
    # Ensure inputs are strings and strip whitespace to prevent matching errors

    rec_type = str(recipe_type).strip()

    # 3. Locate the Row (DSS Version)
    # matching the 'DSS_Major_Version' column
    row = df_dss_recipes[df_dss_recipes["recipe_type"] == rec_type]

    if row.empty:
        return f"Recipe Type '{rec_type}' Not Found"
    else:
        return row.iloc[0]["DSS_v14_recipe_deprecation_status"]


def lookup_python_support(dss_version, python_version, df_dss_python):
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
