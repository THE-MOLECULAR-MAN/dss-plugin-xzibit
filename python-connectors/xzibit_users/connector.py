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

class ConnectorUsers(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys   = ['login', 'displayName',
                      'userProfile', 'groups', 'sourceType', 'email',
                      'creationDate', 'enabled', 'resultingUserProfile',
                      'userProfile']


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.__client.list_users():
            next_row = flatten_dict(item_info, include_keys=self.keys)
            item_id = next_row[self.unique_id_key_name]
            item_handle = self.client.get_user(item_info[self.unique_id_key_name])
            activity_handle = item_handle.get_activity()

            next_row['last_successful_login'] = parse_user_datetime(str(item_handle.get_activity().last_successful_login))
            next_row['last_session_activity'] = parse_user_datetime(str(item_handle.get_activity().last_session_activity))
            next_row['creationDate'] = int_to_datetime(next_row['creationDate'])
            # pp(item_info)
            yield next_row

    def get_read_schema(self):
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date
        return {
            "columns": [
                {
                    "name":    "login", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "displayName", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "userProfile", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "groups", 
                    "type":    "array",
                    "meaning": "JSONArrayMeaning"
                },
                {
                    "name":    "sourceType", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "email", 
                    "type":    "string",
                    "meaning": "Email"
                },
                {
                    "name":    "enabled", 
                    "type":    "boolean",
                    "meaning": "Boolean"
                },
                {
                    "name":    "resultingUserProfile", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "creationDate", 
                    "type":    "date",
                    "meaning": "DatetimeNoTz"
                },
                {
                    "name":    "last_successful_login", 
                    "type":    "date",
                    "meaning": "Date"
                },
                {
                    "name":    "last_session_activity", 
                    "type":    "date",
                    "meaning": "Date"
                }
            ]
        }


    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.__client.list_users())

####################################################################
# Intentionally not implemented, not needed for this type
####################################################################
    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError
