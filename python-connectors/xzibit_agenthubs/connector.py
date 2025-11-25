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

def get_agent_url(project_key, agent_id, agent_version): 
    # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/Data_Dictionary_and_DSS_Instance_datasets_test_project/savedmodels/0XpBITsO/agent/S-Data_Dictionary_and_DSS_Instance_datasets_test_project-0XpBITsO-v1
    # https://honker-design-2.amer.dataiku-sandbox.io       /projects/Data_Dictionary_and_DSS_Instance_datasets_test_project/savedmodels/0XpBITsO/agent/S-Data_Dictionary_and_DSS_Instance_datasets_test_project-0XpBITsO-v1
    base_url = get_dss_base_url()
    return f"{base_url}/projects/{project_key}/savedmodels/{agent_id}/agent/S-{project_key}-{agent_id}-{agent_version}"  

def parse_llm_id(llm_string: str):
    """
    Splits a string by ':' into exactly 3 variables.
    Returns None for missing fields.
    """
    if not llm_string:
        return None, None, None

    # Split the string
    parts = llm_string.split(':')
    
    # Pad the list with None to ensure it has at least 3 elements, 
    # then slice to take exactly the first 3.
    # This handles cases with 1, 2, or 3+ segments gracefully.
    padded = (parts + [None] * 3)[:3]
    
    return padded[0], padded[1], padded[2]


class ConnectorProjects(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
       #  print("[DEBUG agents] Constructor START")
        Connector.__init__(self, config, plugin_config)
        
        self.__client = api_client()
        #self.__unique_id_key_name = 'projectKey'
        #self.__keys   = [self.unique_id_key_name, 'ownerLogin', 'projectStatus', 'contributors', 'name', 
#             'shortDesc', 'description',
#             'tags', 'versionTag.lastModifiedOn', 'tutorialProject']
        self.__objects_list = self.__client.list_project_keys()
        # print("[DEBUG agents] Constructor END")

        
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):       
        

        # print("[generate_rows] END")
                
                
    def get_read_schema(self):
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText
        return None
            
####################################################################
# Same for all instances:
####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        # return len(self.objects_list)
        return None

####################################################################
# Intentionally not implemented, not needed for this type
####################################################################
    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError
