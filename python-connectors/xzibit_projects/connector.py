####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *

####################################################################
# Unique imports for this Class
####################################################################
from datetime import datetime

class ConnectorProjects(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys   = ['projectKey', 'ownerLogin', 'projectStatus', 'contributors', 'name', 
            'shortDesc', 'description',
            'tags', 'versionTag.lastModifiedOn', 'tutorialProject']

            
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.__client.list_projects():
            pp(item_info)
            next_row = flatten_dict(item_info, include_keys=self.keys)
            
            # custom things for this specific class:
            next_row = remove_prefix_from_keys(next_row, 'versionTag.')
            next_row['lastModifiedOn'] = datetime.fromtimestamp(next_row['lastModifiedOn'] // 1000)
            
            # return a single row
            yield next_row

    def get_read_schema(self):
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText
        return {
            "columns": [
                {
                    "name":    "projectKey", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "ownerLogin", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "projectStatus", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "contributors", 
                    "type":    "string",
                    "meaning": "JSONArrayMeaning"
                },
                {
                    "name":    "name", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "shortDesc", 
                    "type":    "string",
                    "meaning": "FreeText"
                },
                {
                    "name":    "description", 
                    "type":    "string",
                    "meaning": "FreeText"
                },
                {
                    "name":    "tags", 
                    "type":    "string",
                    "meaning": "JSONArrayMeaning"
                },
                {
                    "name":    "lastModifiedOn", 
                    "type":    "date",
                    "meaning": "DatetimeNoTz"
                },
                {
                    "name":    "tutorialProject", 
                    "type":    "boolean",
                    "meaning": "Boolean"
                }
            ]
        }
            
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
