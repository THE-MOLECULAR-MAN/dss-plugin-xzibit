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
        self.__unique_id_key_name = 'projectKey'
        self.__keys   = [self.unique_id_key_name, 'ownerLogin', 'projectStatus', 'contributors', 'name', 
            'shortDesc', 'description',
            'tags', 'versionTag.lastModifiedOn', 'tutorialProject']
        self.__objects_list = self.client.list_projects()


        
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):       
        
         # iterate through each object
        for pk, proj_datasets in self.__objects_list.items(): # projects
            project_handle = self.__client.get_project(pk)

            for r in proj_datasets:
                try:
#                    num_rows += 1
                    dataset_handle = project_handle.get_dataset(r.id)
                    next_row = safe_extract_dataset_metadata(dataset_handle, pk)
                    yield next_row

                except Exception as e:
                    print(f"GENERIC EXCEPTION in xzibit_datasets/connector.py - generate_rows with dataset {r.id} in project {pk}: {e} ")
                    # r is of type "dataikuapi.dss.dataset.DSSDataset"
                    # Test failed: com.dataiku.dip.server.controllers.NotFoundException: dataset does not exist:
                    yield {'projectKey': pk,
                               'name':       r.id
                              }

    def get_read_schema(self):
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText
        return None
#         return {
#             "columns": [
#                 {
#                     "name":    "projectKey", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "ownerLogin", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "projectStatus", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "contributors", 
#                     "type":    "array",
#                     "meaning": "JSONArrayMeaning"
#                 },
#                 {
#                     "name":    "name", 
#                     "type":    "string",
#                     "meaning": "Text"
#                 },
#                 {
#                     "name":    "shortDesc", 
#                     "type":    "string",
#                     "meaning": "FreeText"
#                 },
#                 {
#                     "name":    "description", 
#                     "type":    "string",
#                     "meaning": "FreeText"
#                 },
#                 {
#                     "name":    "tags", 
#                     "type":    "array",
#                     "meaning": "JSONArrayMeaning"
#                 },
#                 {
#                     "name":    "lastModifiedOn", 
#                     "type":    "date",
#                     "meaning": "DatetimeNoTz"
#                 },
#                 {
#                     "name":    "tutorialProject", 
#                     "type":    "boolean",
#                     "meaning": "Boolean"
#                 }
#             ]
#         }

            
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
