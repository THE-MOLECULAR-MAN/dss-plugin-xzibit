# This file is the actual code for the custom Python dataset xzibit_projects
from datetime import datetime

from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key

"""
A custom Python dataset is a subclass of Connector.
"""
class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        Constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        try:
            self.client = api_client()
            self.objects_list = self.client.list_projects()
            assert isinstance(self.objects_list, list), "self.objects_list must be of type list"
        except Exception as e:
            print(f"CONSTRUCTOR EXCEPTION: {e}")
        finally:
            self.count = len(self.objects_list)
    

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.
        """
        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.
        """
#         for project_info in self.client.list_projects():
#             next_project = flatten_dict(project_info, 
#                                include_keys=['projectKey', 'ownerLogin', 'projectStatus', 'contributors', 'name', 'projectLocation', 'projectStatus', 'shortDesc', 'tags', 'versionTag.lastModifiedOn', 'tutorialProject'])
#             next_project = remove_prefix_from_keys(next_project, 'versionTag.')
#             next_project['lastModifiedOn'] = datetime.fromtimestamp(next_project['lastModifiedOn'] // 1000)
#             yield next_project
            
       
        keys = ['projectKey', 'ownerLogin', 'projectStatus', 'contributors', 'name', 
                'projectLocation', 'projectStatus', 'shortDesc', 
                'tags', 'versionTag.lastModifiedOn', 'tutorialProject']
        
        for item_info in self.objects_list:
            try:
                next_row = flatten_dict(item_info, include_keys=keys)
                next_row = remove_prefix_from_keys(next_row, 'versionTag.')
                next_row['lastModifiedOn'] = datetime.fromtimestamp(next_row['lastModifiedOn'] // 1000)

            except Exception as e:
                print(f"Exception {e} with item_info:")
                pp(item_info)
                next_row = list_to_error_dict(keys)
                
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
        return self.count
