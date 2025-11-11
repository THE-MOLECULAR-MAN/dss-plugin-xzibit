
# pretty print dictionaries for debugging - don't remove at this time.
from pprint import pprint as pp

import os

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
