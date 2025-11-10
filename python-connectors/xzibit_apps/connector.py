# This file is the actual code for the custom Python dataset xzibit_apps
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key

"""
A custom Python dataset is a subclass of Connector.
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
        """
        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.
        """
        # from pprint import pprint as pp
            
        keys = ['appId', 'appVersion', 'label', 'origin', 'shortDesc', 
                'tags', 'isAppImg', 'instanceCount', 'useAsRecipe', 
                'onlyLimitedVisibility']
        for app_info in self.client.list_apps():            
            try:
                next_row = flatten_dict(app_info, 
                               include_keys=keys)
            except Exception as e:
                print(f"Exception {e} with app_info:")
                pp(app_info)
                next_row = list_to_error_dict(keys)
                next_row['appId'] = connection_info.get('appId', 'NO_NAME')
            finally:
                yield next_row



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
        return len(self.client.list_apps())
