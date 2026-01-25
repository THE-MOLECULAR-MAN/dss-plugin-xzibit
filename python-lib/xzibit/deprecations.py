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


def load_python_support(filepath="./DSS_version_python_support.csv"):
    """TBD"""
    # check if file exists
    try:
        df = pd.read_csv(filepath, dtype=str)
    except FileNotFoundError:
        df = None

    return df


def lookup_python_support(dss_version, python_version, df=load_python_support()):
    """TBD"""
    if df is None:
        return "unknown"

    row = df[
        (df["DSS_Version"] == dss_version) & (df["Python_Version"] == python_version)
    ]

    if row.empty:
        return None

    return row.iloc[0]["Support_Status"]


df_python_support = load_python_support()
