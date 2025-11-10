
# pretty print dictionaries for debugging - don't remove
from pprint import pprint as pp


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
