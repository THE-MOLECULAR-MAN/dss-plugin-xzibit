####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *


class ConnectorConnections(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys   = ['name', 'type', 'usableBy', 'allowWrite',
                'credentialsMode', 'params.credentialsMode', 'creationTag.lastModifiedBy.login', 'creationTag.lastModifiedBy.lastModifiedOn', 'params.authType',
                'params.db', 'params.defaultSchema', 'params.role', 'params.warehouse', 'params.scope']
        # self.__objects_list = self.__client.list_connections(as_type='listitems')

            
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.__client.list_connections(as_type='listitems'):
            #pp(item_info)
            next_row = flatten_dict(item_info, include_keys=self.__keys)
            yield next_row

            
####################################################################
# Same for all instances:
####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.__client.list_connections())

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
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return None
#         return {
#             "columns": [
#                 {
#                     "name":    "name", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "type", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "usableBy", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "allowWrite", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "credentialsMode", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 }
#             ]
#         }
           