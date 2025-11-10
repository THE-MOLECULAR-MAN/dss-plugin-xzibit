# This file is the actual code for the custom Python dataset xzibit_plugins
import json
# import the base class for the custom dataset
from dataiku import api_client
from dataiku.connector import Connector

from xzibit.connector import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key
# def get_values_for_key(ld, k):
#     """
#     Extract the unique values for a given key from a list of dictionaries.

#     Args:
#         ld (list[dict]): List of dictionaries.
#         k (str): The key to extract values for.

#     Returns:
#         set: A set of unique values for the specified key.
#     """
#     return {d[k] for d in ld if isinstance(d, dict) and k in d}

# def get_values_from_list_of_dicts(list_of_dicts):
#     """
#     Extract a list of unique values from a list of dictionaries.

#     Args:
#         list_of_dicts (list[dict]): List containing dictionaries.

#     Returns:
#         list: List of unique values (preserving order of first appearance).
#     """
#     seen = set()
#     values = []
#     for d in list_of_dicts:
#         if isinstance(d, dict):
#             for v in d.values():
#                 if v not in seen:
#                     seen.add(v)
#                     values.append(v)
#     return values

# def flatten_dict(d, parent_key='', sep='.', include_keys=None):
#     """
#     Recursively flattens a nested dictionary and optionally filters which keys to include.

#     Ex: flatten_dict(data, include_keys=['label', 'url', 'version'])
    
#     Args:
#         d (dict): The input dictionary to flatten.
#         parent_key (str): Used internally for recursion; do not set manually.
#         sep (str): Separator for concatenated keys. Default is '.'.
#         include_keys (list[str] | None): 
#             Optional list of keys (or substrings) to include in the final output. 
#             If None, all keys are included.

#     Returns:
#         dict: A flattened dictionary.
#     """
#     items = []
#     for k, v in d.items():
#         new_key = f"{parent_key}{sep}{k}" if parent_key else k
#         if isinstance(v, dict):
#             items.extend(flatten_dict(v, new_key, sep, include_keys).items())
#         else:
#             # if filtering, include only matching keys (by substring)
#             if include_keys is None or any(frag in new_key for frag in include_keys):
#                 items.append((new_key, v))
#     return dict(items)


# def remove_prefix_from_keys(d, prefix, recursive=True):
#     """
#     Remove a given prefix from all keys in a dictionary.

#     Args:
#         d (dict): Input dictionary.
#         prefix (str): The prefix to remove (exact match at start of key).
#         recursive (bool): If True, will also traverse nested dictionaries.

#     Returns:
#         dict: A new dictionary with the prefix removed from all matching keys.
#     """
#     new_dict = {}
#     for k, v in d.items():
#         # Remove the prefix if the key starts with it
#         new_key = k[len(prefix):] if k.startswith(prefix) else k
#         # Strip a leading separator if present (e.g., '.' or '_')
#         if new_key.startswith('.') or new_key.startswith('_'):
#             new_key = new_key[1:]
#         # Recurse into nested dicts if enabled
#         if recursive and isinstance(v, dict):
#             new_dict[new_key] = remove_prefix_from_keys(v, prefix, recursive)
#         else:
#             new_dict[new_key] = v
#     return new_dict

"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant.
"""
class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.client = api_client()
    

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        If you do provide a schema here, all columns defined in the schema
        will always be present in the output (with None value),
        even if you don't provide a value in generate_rows

        The schema must be a dict, with a single key: "columns", containing an array of
        {'name':name, 'type' : type}.

        Example:
            return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

        Supported types are: string, int, bigint, float, double, date, boolean
        """
#        return self.schema
        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        for plugin_info in self.client.list_plugins():
            next_plugin = flatten_dict(plugin_info, 
                               include_keys=['meta.label', 'id', 'version', 'meta.author', 'meta.tags', 'meta.description', 'isDev'])
            next_plugin = remove_prefix_from_keys(next_plugin, 'meta.')
            plugin_handle = self.client.get_plugin(next_plugin['id'])
            list_of_usages = plugin_handle.list_usages().get_raw()['usages']
            if len(list_of_usages) == 0:
                next_plugin['usages'] = []
            else:
                next_plugin['usages'] = list(get_values_for_key(list_of_usages, 'projectKey')) 
            yield next_plugin


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise NotImplementedError


    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []


    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise NotImplementedError


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        return len(self.client.list_plugins())
