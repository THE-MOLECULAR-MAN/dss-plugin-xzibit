import os
import re
from datetime import datetime

import dataikuapi
from dataikuapi.utils import DataikuException



# pretty print dictionaries for debugging - don't remove at this time.
from pprint import pprint as pp
from json   import dumps  as jd

def safe_extract_dataset_metadata(dataset_handle):
    """x"""
    # print('safe_extract_dataset_metadata START')
    assert isinstance(dataset_handle, dataikuapi.dss.dataset.DSSDataset), f"safe_extract_dataset_metadata - Assertion failed: Expecting DSSDataset, got {type(dataset_handle)}"
    
    keys = ['name', 'type', 'formatType', 'params.connection',
           'managed', 'params.mode', 'params.table', 'params.schema', 'params.database',
           'params.path', 
           'creationTag.lastModifiedBy.login', 'creationTag.lastModifiedOn',
           'versionTag.lastModifiedBy.login',  'versionTag.lastModifiedOn',
           'shortDesc', 'description', 'params.metastoreDatabaseName',
           'params.folderSmartId', 'tags', 'featureGroup',
          ]
    try:
        dataset_metadata = {}
        dataset_metadata['id']     = dataset_handle.id
        dataset_metadata['name']   = dataset_handle.name
        dataset_metadata['exists'] = dataset_handle.exists()
        
        if not dataset_metadata['exists']:
            print('safe_extract_dataset_metadata - dataset does NOT exist.')
            return dataset_metadata
            
        try:
            raw_data = dataset_handle.get_info().get_raw() # returns dict, can throw com.dataiku.dip.server.controllers.NotFoundException
        except Exception as e:
            print(f"safe_extract_dataset_metadata - EXCEPTION at dataset_handle.get_info().get_raw()")
            return dataset_metadata

        # key_mapping.update(list_keys_recursive(raw_data)) # debugging, mapping out all the different keys depending on the type of dataset

        try:
            dataset_metadata_new = extract_nested_keys(raw_data, keys) # NOT causing exception
            # pp(dataset_metadata) # NOT causing exception
            #dataset_metadata = dataset_metadata | x # Python 3.9+  # maybe causing exception
            #dataset_metadata = {**dataset_metadata, **x} # more compatible
            #pp(dataset_metadata)
            dataset_metadata.update(dataset_metadata_new)

        except Exception as e:
            print(f"safe_extract_dataset_metadata - EXCEPTION at extract_nested_keys")
            return dataset_metadata
        
        #print(f"about to do the tricky stuff...")

        # safe_extract_dataset_metadata - Generic exception 'NoneType' object has no attribute 'get'
        #dataset_metadata['num_metrics_checks'] = len(raw_data.get('metricsChecks', {}).get('checks', []))
        #dataset_metadata['num_columns']        = len(raw_data.get('schema', {}).get('columns', []))
        #dataset_metadata['column_names']       = [col["name"] for col in raw_data.get("schema", {}).get("columns", []) if "name" in col]
#         dataset_metadata['creationTag.lastModifiedOn'] = int_to_datetime(dataset_metadata.get('creationTag.lastModifiedOn', None))
#         dataset_metadata['versionTag.lastModifiedOn']  = int_to_datetime(dataset_metadata.get('versionTag.lastModifiedOn',  None))
        
        print(f"safe_extract_dataset_metadata successful end")

    except DataikuException as e:
        print(f"safe_extract_dataset_metadata - Dataiku exception {e}")
        dataset_metadata['exists'] = "EXCEPTION DataikuException"
        return dataset_metadata
    except Exception as e:
        print(f"safe_extract_dataset_metadata - Generic exception {e}")
        dataset_metadata['exists'] = "EXCEPTION"
        return dataset_metadata
    finally:
        print(f"safe_extract_dataset_metadata - FINALLY")        
        return dataset_metadata



def print_sorted_strings(s: set[str]) -> None:
    """
    Print all strings in a set, sorted alphabetically (case-insensitive), one per line.
    """
    for item in sorted(s, key=str.lower):
        print(item)

def list_keys_recursive(d: dict, parent_key: str = '') -> list[str]:
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

def extract_nested_keys(d: dict, keys: list[str]) -> dict[str, object]:
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
        for key in key_path.split('.'):
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data

    return {key: get_nested_value(d, key) for key in keys}


def int_to_datetime(timestamp: int) -> datetime:
    """
    Convert an integer timestamp (in seconds or milliseconds)
    into a datetime.datetime object (UTC).
    """
    # Detect if the timestamp is in milliseconds
    if not isinstance(timestamp, int):
        t = str(type(timestamp))
        print(f"ERROR: int_to_datetime - not an integer: {timestamp} - {t}")
        return None

    if timestamp > 1e12:
        timestamp /= 1000  # convert to seconds
    
    return datetime.utcfromtimestamp(timestamp)


def parse_user_datetime(dt_str: str) -> datetime:
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

    
def get_jq_value(data: dict, jq_path: str):
    """
    Traverse a nested dict using a jq-style path like 'a.b.c'.
    Returns the value if found, else None.
    """
    try:
        keys = jq_path.split('.')
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data
    except Exception:
        return None

def list_to_error_dict(strings: list[str], value="error") -> dict[str, str]:
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
    for dirpath, dirnames, filenames in os.walk(path, onerror=None, followlinks=False):
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

def flatten_dict(d, parent_key='', sep='.', include_keys=None):
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
        new_key = k[len(prefix):] if k.startswith(prefix) else k
        # Strip a leading separator if present (e.g., '.' or '_')
        if new_key.startswith('.') or new_key.startswith('_'):
            new_key = new_key[1:]
        # Recurse into nested dicts if enabled
        if recursive and isinstance(v, dict):
            new_dict[new_key] = remove_prefix_from_keys(v, prefix, recursive)
        else:
            new_dict[new_key] = v
    return new_dict
