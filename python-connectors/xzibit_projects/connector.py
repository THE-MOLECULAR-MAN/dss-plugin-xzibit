from datetime import datetime

from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key, pp

class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """Constructor"""
        Connector.__init__(self, config, plugin_config)
        self.client = api_client()
        self.unique_id_key_name = 'projectKey'
        self.keys   = [self.unique_id_key_name, 'ownerLogin', 'projectStatus', 'contributors', 'name', 
            'projectLocation', 'projectStatus', 'shortDesc', 
            'tags', 'versionTag.lastModifiedOn', 'tutorialProject']
        self.objects_list = self.client.list_projects()
        self.count = len(self.objects_list)

            
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        for item_info in self.objects_list:
            try:
                next_row = flatten_dict(item_info, include_keys=self.keys)
                next_row = remove_prefix_from_keys(next_row, 'versionTag.')
                next_row['lastModifiedOn'] = datetime.fromtimestamp(next_row['lastModifiedOn'] // 1000)

            except Exception as e:
                print(f"Exception {e} with item_info:")
                pp(item_info)
                next_row = list_to_error_dict(keys)
                
            finally:
                yield next_row


    def get_records_count(self, partitioning=None, partition_id=None):
        return self.count

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
