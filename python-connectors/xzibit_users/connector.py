####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key, parse_user_datetime
from xzibit.utils import pp


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
        
        self.client = api_client()
        self.unique_id_key_name = 'login'
        self.keys   = [self.unique_id_key_name, 'displayName',
                      'userProfile', 'groups', 'sourceType', 'email',
                      'creationDate', 'enabled', 'resultingUserProfile',
                      'userProfile']
        self.objects_list = self.client.list_users()


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.objects_list:
            next_row = flatten_dict(item_info, include_keys=self.keys)
            item_id = next_row[self.unique_id_key_name]
            item_handle = self.client.get_user(item_info[self.unique_id_key_name])
            
            x = item_handle.get_activity().last_successful_login
            print(x)
            print(str(type(x)))
            
            next_row['last_successful_login'] = parse_user_datetime(x)
            # next_row['last_session_activity'] = parse_user_datetime(item_handle.get_activity().last_session_activity)            
            # pp(item_info)
            # return a single row
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
