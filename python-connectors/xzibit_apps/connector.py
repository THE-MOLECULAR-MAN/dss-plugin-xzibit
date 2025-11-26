####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *


####################################################################
# Unique imports for this Class
####################################################################
# none.

class ConnectorApps(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys   = ['appId', 'appVersion', 'label', 
                 'origin', 'shortDesc', 
                'tags', 'isAppImg', 'instanceCount', 'useAsRecipe', 
                'onlyLimitedVisibility']


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        # iterate through each object
        for item_info in self.__client.list_apps():
            next_row = flatten_dict(item_info, include_keys=self.__keys)
            yield next_row


    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.__client.list_apps())

            
####################################################################
# Same for all instances:
####################################################################

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
