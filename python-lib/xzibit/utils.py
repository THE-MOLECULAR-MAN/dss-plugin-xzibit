import os
import re
import glob
import shutil
from datetime import datetime

import dataiku
import dataikuapi
from dataikuapi.utils import DataikuException

# pretty print dictionaries for debugging - don't remove at this time.
from pprint import pprint as pp
from json import dumps as jd

JAVA_NOT_IMPLEMENTED = "com.dataiku.dip.utils.NotImplementedException"


def compare_major_minor_versions(version_a: str, version_b: str) -> int:
    """
    Compares two 'major.minor' version strings.

    Args:
        version_a (str): The first version (e.g., "3.7")
        version_b (str): The second version (e.g., "3.11")

    Returns:
        int:
             1 if version_a > version_b
             0 if version_a == version_b
            -1 if version_a < version_b

    Raises:
        ValueError: If inputs are not in 'int.int' format.
    """
    try:
        # Split by period and convert to tuple of integers
        # "3.11" -> (3, 11)
        tuple_a = tuple(map(int, str(version_a).split(".")))
        tuple_b = tuple(map(int, str(version_b).split(".")))

        if tuple_a > tuple_b:
            return 1
        elif tuple_a < tuple_b:
            return -1
        else:
            return 0
    except ValueError:
        # Handle cases where version might be "Unknown" or empty
        # Returning -2 or raising error depends on your failure strategy
        return -2


def extract_keys(d, v=True, key_prefix="allow"):
    """
    Finds keys in dictionary 'd' that start with 'key_prefix' and have value 'v'.
    Returns a list of these keys with the prefix removed.

    Used in extracting an LLM connection's list of allowed LLM models
    """
    result = []

    for key, value in d.items():
        # Check if key starts with the prefix AND value matches v
        if key.startswith(key_prefix) and value == v:
            # Remove the prefix using string slicing
            stripped_key = key[len(key_prefix) :]
            result.append(stripped_key)

    return result


def recursive_search_all(data, s):
    """
    Recursively searches a dictionary, list, or other object for keys or
    values that match the string 's'.

    Args:
        data (dict | list | object): The structure to search.
        s (str): The key or value string to search for.

    Returns:
        list: A list of all results found, or None if no matches are found.
    """
    results = []

    # --- 1. Handle Dictionaries ---
    if isinstance(data, dict):
        for key, value in data.items():
            # Check the Key
            if key == s:
                results.append(value)

            # Check the Value
            if value == s:
                results.append(value)

            # Recurse on the Value (Handles nested dicts, lists, etc.)
            nested_results = recursive_search_all(value, s)
            if nested_results is not None:
                results.extend(nested_results)

    # --- 2. Handle Lists and Tuples ---
    elif isinstance(data, (list, tuple)):
        for item in data:
            # Check the Value
            if item == s:
                results.append(item)

            # Recurse on the Item
            nested_results = recursive_search_all(item, s)
            if nested_results is not None:
                results.extend(nested_results)

    # --- 3. Handle Primitive/Scalar Types (Final return check) ---
    # No more recursion is possible here. The value check above covers this.

    # Final check: If the results list is empty, return None as requested
    if not results:
        return None
    else:
        return results


def replace_empty_arrays_sets_with_none(x):
    """TBD"""
    try:
        if (
            (x is None)
            or (isinstance(x, str) and x == "[]")
            or (isinstance(x, list) and len(x) == 0)
            or (isinstance(x, str) and x == "{}")
            or (isinstance(x, set) and len(x) == 0)
            or (isinstance(x, dict) and (not x))
        ):
            return None
    except Exception as e:
        print(
            f"[replace_empty_arrays_sets_with_none] EXCEPTION: {str(type(x))} {str(x)} {e}"
        )
        return x
    return x


def get_dss_external_url():
    """TBD"""
    # 1. Initialize the client (connects to local instance)
    client = dataiku.api_client()

    # 2. Retrieve General Settings (Requires Admin permissions)
    # This corresponds to the "Administration > Settings > General" page
    settings = client.get_general_settings()

    # 3. Extract the 'studioExternalUrl' from the raw settings dictionary
    # This key holds the value of the "DSS URL" field
    dss_url = settings.get_raw().get("studioExternalUrl")

    if dss_url:
        return dss_url

    return None


def get_dss_url_from_env():
    """TBD"""
    # Attempt to retrieve host and port from environment variables
    ext_host = os.environ.get("DKU_BACKEND_EXT_HOST")
    base_port = os.environ.get("DKU_BASE_PORT")

    if ext_host and base_port:
        # Note: You may need to infer the scheme (http vs https)
        # based on your knowledge of the instance setup.
        s = f"https://{ext_host}:{base_port}"
        return s
    return None


def get_dss_url_from_global_vars():
    """TBD"""
    client = dataiku.api_client()

    # Retrieve global variables (accessible to all users)
    global_vars = client.get_variables()

    # Check for common naming conventions like 'dss_url', 'public_url', or 'instance_url'
    return global_vars.get("dss_url") or global_vars.get("public_url")


def get_dss_base_url():
    """returns the base URL for the local node, without a trailing slash"""
    res = (
        get_dss_url_from_env()
        or get_dss_external_url()
        or get_dss_url_from_global_vars()
    )
    if len(res) > 0:
        return res.rstrip("/")
    else:
        return None


def safe_extract_dataset_metadata(
    dataset_handle, pk, get_column_lineage=False
):  # , get_data_quality_rules=False):
    """SLOW! Adds 1.36 seconds per dataset (row) on average"""
    assert isinstance(
        dataset_handle, dataikuapi.dss.dataset.DSSDataset
    ), f"safe_extract_dataset_metadata - Assertion failed: Expecting DSSDataset, got {type(dataset_handle)}"

    keys = [
        "name",
        "type",
        "formatType",
        "params.connection",
        "managed",
        "params.mode",
        "params.table",
        "params.schema",
        "params.path",
        "creationTag.lastModifiedBy.login",
        "creationTag.lastModifiedOn",
        "versionTag.lastModifiedBy.login",
        "versionTag.lastModifiedOn",
        "shortDesc",
        "description",
        "params.metastoreDatabaseName",
        "params.folderSmartId",
        "tags",
        "featureGroup",
    ]
    try:
        dataset_metadata = {}
        dataset_metadata["projectKey"] = pk
        dataset_metadata["id"] = dataset_handle.id
        dataset_metadata["name"] = dataset_handle.name
        dataset_metadata["exists"] = dataset_handle.exists()

        if not dataset_metadata["exists"]:
            return dataset_metadata

        try:
            raw_data = (
                dataset_handle.get_info().get_raw()
            )  # returns dict, can throw com.dataiku.dip.server.controllers.NotFoundException
            raw_data = raw_data.get("dataset", {})  # fix for get_info
        except Exception:
            print(
                f"safe_extract_dataset_metadata - EXCEPTION at dataset_handle.get_info().get_raw()"
            )
            dataset_metadata["exists"] = False
            return dataset_metadata

        try:

            dataset_metadata_new = extract_nested_keys(
                raw_data, keys
            )  # NOT causing exception
            dataset_metadata.update(dataset_metadata_new)  # def not causing exception

        except Exception:
            print(f"safe_extract_dataset_metadata - EXCEPTION at extract_nested_keys")
            dataset_metadata["exists"] = "EXCEPTION 2"
            return dataset_metadata

        dataset_metadata["num_metrics_checks"] = len(
            raw_data.get("metricsChecks", {}).get("checks", [])
        )
        dataset_metadata["num_columns"] = len(
            raw_data.get("schema", {}).get("columns", [])
        )
        dataset_metadata["column_names"] = [
            col["name"]
            for col in raw_data.get("schema", {}).get("columns", [])
            if "name" in col
        ]
        dataset_metadata["creationTag.lastModifiedOn"] = int_to_datetime(
            dataset_metadata.get("creationTag.lastModifiedOn", None)
        )
        dataset_metadata["versionTag.lastModifiedOn"] = int_to_datetime(
            dataset_metadata.get("versionTag.lastModifiedOn", None)
        )

    except DataikuException as e:
        print(f"safe_extract_dataset_metadata - Dataiku exception {e}")
        dataset_metadata["exists"] = "EXCEPTION 3"
        return dataset_metadata
    except Exception as e:
        print(f"safe_extract_dataset_metadata - Generic exception {e}")
        dataset_metadata["exists"] = "EXCEPTION 4"
        return dataset_metadata
    finally:
        return dataset_metadata


def print_sorted_strings(s):
    """
    Print all strings in a set, sorted alphabetically (case-insensitive), one per line.
    """
    for item in sorted(s, key=str.lower):
        print(item)


def list_keys_recursive(d, parent_key=""):
    """
    Recursively list all keys in a nested dictionary using dot notation,
    ignoring list indices (e.g., schema.columns[0].name -> schema.columns.name).

    Args:
        d (dict): The dictionary to traverse.
        parent_key (str): Used internally to build nested key paths.

    Returns:
        list[str]: List of all keys in dot-delimited form.
    """
    keys = []
    if not isinstance(d, dict):
        t = str(type(d))
        print(f"ERROR: list_keys_recursive - not a dict: {d} - {t}")
        return None

    for k, v in d.items():
        full_key = f"{parent_key}.{k}" if parent_key else k
        keys.append(full_key)

        if isinstance(v, dict):
            keys.extend(list_keys_recursive(v, full_key))
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    # Recurse without adding an index
                    keys.extend(list_keys_recursive(item, full_key))
    return keys


def extract_nested_keys(d, keys):
    """
    Extract nested keys (dot-separated) from a dictionary.
    If a key path does not exist, its value is None in the returned dictionary.

    Args:
        d (dict): The source dictionary.
        keys (list[str]): List of (possibly nested) keys, separated by dots.

    Returns:
        dict[str, object]: Dictionary of {key_path: value or None}.
    """

    def get_nested_value(data, key_path):
        """Safely get a nested value from a dict using dot-separated keys."""
        for key in key_path.split("."):
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data

    return {key: get_nested_value(d, key) for key in keys}


def int_to_datetime(timestamp):
    """
    Convert an integer timestamp (in seconds or milliseconds)
    into a datetime.datetime object (UTC).
    """
    # Detect if the timestamp is in milliseconds
    if not isinstance(timestamp, int):
        timestamp = 0

    # can cause a bug in like 50k years from now? ;-)
    if timestamp > 1e12:
        timestamp /= 1000  # convert to seconds

    return datetime.utcfromtimestamp(timestamp)


def parse_user_datetime(dt_str):
    """
    Convert a string like '2025-11-11 15:08:36.439000+00:00'
    into a timezone-aware datetime.datetime object.
    Returns None if parsing fails.
    """
    try:
        # Replace space with 'T' for fromisoformat compatibility
        dt_str = dt_str.replace(" ", "T")
        return datetime.fromisoformat(dt_str)
    except ValueError:
        return None


def get_jq_value(data, jq_path):
    """
    Traverse a nested dict using a jq-style path like 'a.b.c'.
    Returns the value if found, else None.
    """
    try:
        keys = jq_path.split(".")
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data
    except Exception:
        return None


def list_to_error_dict(strings, value="error"):
    """
    Convert a list of strings into a dictionary where each string is a key
    and each value is the default string 'error'.

    Args:
        strings (list[str]): List of strings to use as dictionary keys.

    Returns:
        dict[str, str]: Dictionary with each key mapped to 'error'.
    """
    return {s: value for s in strings}


def get_path_size_megabytes(path):
    """TBD"""
    # Convert bytes → megabytes (1 MB = 1024 * 1024 bytes)
    size_mb = get_path_size(path) / (1024 * 1024)
    return round(size_mb, 1)


def get_path_size(path):
    """
    Recursively calculate the total size of a file or directory (in bytes).

    Args:
        path (str): Absolute path to a file or directory on the local filesystem.

    Returns:
        int: Total size in bytes.
    """
    total_size = 0

    if not os.path.exists(path):
        return 0
        # raise FileNotFoundError(f"Path does not exist: {path}")

    # If it's a file, just return its size directly
    if os.path.isfile(path):
        return os.path.getsize(path)

    # Otherwise, walk through all subdirectories and files
    for dirpath, _, filenames in os.walk(path, onerror=None, followlinks=False):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.path.getsize(fp)
            except (OSError, FileNotFoundError):
                # Ignore files that disappear or are unreadable
                pass

    return total_size


def get_values_for_key(ld, k):
    """
    Extract the unique values for a given key from a list of dictionaries.

    Args:
        ld (list[dict]): List of dictionaries.
        k (str): The key to extract values for.

    Returns:
        set: A set of unique values for the specified key.
    """
    return {d[k] for d in ld if isinstance(d, dict) and k in d}


def get_values_from_list_of_dicts(list_of_dicts):
    """
    Extract a list of unique values from a list of dictionaries.

    Args:
        list_of_dicts (list[dict]): List containing dictionaries.

    Returns:
        list: List of unique values (preserving order of first appearance).
    """
    seen = set()
    values = []
    for d in list_of_dicts:
        if isinstance(d, dict):
            for v in d.values():
                if v not in seen:
                    seen.add(v)
                    values.append(v)
    return values


def flatten_dict(d, parent_key="", sep=".", include_keys=None):
    """
    Recursively flattens a nested dictionary and optionally filters which keys to include.

    Ex: flatten_dict(data, include_keys=['label', 'url', 'version'])

    Args:
        d (dict): The input dictionary to flatten.
        parent_key (str): Used internally for recursion; do not set manually.
        sep (str): Separator for concatenated keys. Default is '.'.
        include_keys (list[str] | None):
            Optional list of keys (or substrings) to include in the final output.
            If None, all keys are included.

    Returns:
        dict: A flattened dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep, include_keys).items())
        else:
            # if filtering, include only matching keys (by substring)
            if include_keys is None or any(frag in new_key for frag in include_keys):
                items.append((new_key, v))
    return dict(items)


def remove_prefix_from_keys(d, prefix, recursive=True):
    """
    Remove a given prefix from all keys in a dictionary.

    Args:
        d (dict): Input dictionary.
        prefix (str): The prefix to remove (exact match at start of key).
        recursive (bool): If True, will also traverse nested dictionaries.

    Returns:
        dict: A new dictionary with the prefix removed from all matching keys.
    """
    new_dict = {}
    for k, v in d.items():
        # Remove the prefix if the key starts with it
        new_key = k[len(prefix) :] if k.startswith(prefix) else k
        # Strip a leading separator if present (e.g., '.' or '_')
        if new_key.startswith(".") or new_key.startswith("_"):
            new_key = new_key[1:]
        # Recurse into nested dicts if enabled
        if recursive and isinstance(v, dict):
            new_dict[new_key] = remove_prefix_from_keys(v, prefix, recursive)
        else:
            new_dict[new_key] = v
    return new_dict


def clear_pip_tmp():
    """
    This function deletes all the temporary files created by
    Pip during the installation process. They are not always cleared
    and when dealing with dozens of Code Environments,
    it can fill up the hard disk very quickly.
    It assumpes they're located in /tmp/pip-*
    """

    for d in glob.glob("/tmp/pip-*"):
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        else:
            os.remove(d)


def get_python_recipe_code_env(recipe):
    """
    Retrieves the name of the code environment used by a Python recipe
    by inspecting the raw settings payload.

    Args:
        recipe (dataikuapi.dss.recipe.DSSRecipe): The handle to the recipe object.

    Returns:
        str: The name of the code environment if the recipe is a 'python' recipe
             and an environment is explicitly selected.
             Returns "" (empty string) if:
               - It is not a Python recipe.
               - It uses the project default environment (Inherit).
               - It uses the DSS built-in environment.
    """
    try:
        # Get the settings handle
        settings = recipe.get_settings()
        # Access the underlying JSON dictionary directly to avoid AttributeError
        # on missing helper methods.
        payload = settings.get_recipe_raw_definition()

        # 1. Verify this is a Python recipe
        if payload.get("type") != "python":
            return ""

        # 2. Navigate the params > envSelection structure
        # The structure typically looks like:
        # { "params": { "envSelection": { "envMode": "EXPLICIT_ENV", "envName": "..." } } }
        params = payload.get("params", {})
        env_selection = params.get("envSelection", {})

        # 3. Check the mode. We only return a name if it's set to EXPLICIT_ENV.
        # Other modes (INHERIT, DSS_BUILTIN) imply no specific env name to return.
        # if env_selection.get('envMode','') == 'EXPLICIT_ENV':
        return env_selection.get("envName", "")

        # return ""

    except Exception:
        # Fail gracefully if the payload structure is unexpected
        return ""
