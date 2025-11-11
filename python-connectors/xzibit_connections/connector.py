####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key
from xzibit.utils import pp


####################################################################
# Unique imports for this Class
####################################################################
from datetime import datetime

class ConnectorConnections(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        
        self.client = api_client()
        self.unique_id_key_name = 'projectKey'
        self.keys   = [self.unique_id_key_name, 'ownerLogin', 'projectStatus', 'contributors', 'name', 
            'projectLocation', 'projectStatus', 'shortDesc', 
            'tags', 'versionTag.lastModifiedOn', 'tutorialProject']
        self.objects_list = self.client.list_projects()

            
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.objects_list:
            next_row = flatten_dict(item_info, include_keys=self.keys)
            
            # custom things for this specific class:
            next_row = remove_prefix_from_keys(next_row, 'versionTag.')
            next_row['lastModifiedOn'] = datetime.fromtimestamp(next_row['lastModifiedOn'] // 1000)
            
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

    


# This file is the actual code for the custom Python dataset xzibit_connections
from datetime import datetime

# import pprint

from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key, pp

"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant.
"""

class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        Constructor
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
        keys = ['name', 'type', 'usableBy', 'allowWrite', 'allowedGroups', 
                'credentialsMode', 'name', 'type', 'usableBy']
        for connection_info in self.client.list_connections(as_type='listitems'):
            try:
                next_row = flatten_dict(connection_info, include_keys=keys)
            except Exception as e:
                print(f"Exception {e} with connection_info:")
                pp(connection_info)
                next_row = list_to_error_dict(keys)
                next_row['name'] = connection_info.get('name', 'NO_NAME')
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
        return len(self.client.list_connections(as_type='listitems'))
