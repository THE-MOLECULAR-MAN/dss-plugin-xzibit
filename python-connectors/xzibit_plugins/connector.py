####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *

####################################################################
# Unique imports for this Class
####################################################################
#

class ConnectorPlugins(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        
        self.client = api_client()
        self.unique_id_key_name = 'id'
        self.keys   = [self.unique_id_key_name, 'meta.label', 'version', 'meta.author', 'meta.tags', 'meta.description', 'isDev']
        self.objects_list = self.client.list_plugins()

            
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.objects_list:
            try:
                next_row = flatten_dict(item_info, include_keys=self.keys)

                # custom things for this specific class:
                next_row = remove_prefix_from_keys(next_row, 'meta.')

                plugin_handle = self.client.get_plugin(next_row['id'])

                list_of_usages = plugin_handle.list_usages().get_raw()['usages']

                if len(list_of_usages) == 0:
                    next_row['project_usages'] = []
                else:
                    next_row['project_usages'] = list(get_values_for_key(list_of_usages, 'projectKey')) 

                next_row['total_usages'] = len(list_of_usages)
            except Exception as e:
                print(f"Exception {e} with plugin_info:")
                pprint(plugin_info)
                next_row = list_to_error_dict(keys)
            finally:
                yield next_row

####################################################################
# Same for all instances:
####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.objects_list)

####################################################################
# Intentionally not implemented, not needed for this type
####################################################################
    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError

    def get_read_schema(self):
        return None
