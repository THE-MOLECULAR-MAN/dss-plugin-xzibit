"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import remove_prefix_from_keys, flatten_dict, get_dss_base_url, pp

####################################################################
# Unique imports for this Class
####################################################################
from datetime import datetime


class ConnectorProjects(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        
    

   
    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        
        # iterate through projects
        for project_key in self.__client.list_project_keys():
            try:
                project = self.__client.get_project(project_key)

                # exit early if exceeded the number of records requested
                if records_limit > 0 and records_generated >= records_limit:
                    return

                # objects that can use LLMs:
                # agents
                for agent_handle in project.list_agents(as_type='objects'):
                    try: 
                        next_row = {"projectKey": project_key}
                        agent_data = agent_handle.get_settings().get_raw()


                        next_row['dss_object_id'] = agent_handle.id
                        next_row['dss_object_name'] = agent_handle.name
                    except Exception as e:
                        print(f"[EXCEPTION]")
                    
                
                
            except Exception as e:
                    print(
                        f"[generate_rows] [UNEXPECTED EXCEPTION project] {e} with object"
                    )
                  
    

    def get_read_schema(self):
        """Returns the read schema for TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {'columns': [{'meaning': 'Text', 'name': 'projectKey', 'type': 'string'},
             {'meaning': 'Text', 'name': 'kb_id', 'type': 'string'},
             {'meaning': 'Text', 'name': 'kb_name', 'type': 'string'},
             {'meaning': 'Text', 'name': 'kb_embeddingLLMId', 'type': 'string'},
             {'meaning': 'Text', 'name': 'retrieverType', 'type': 'string'},
             {'meaning': 'Text',
              'name': 'kb_vectorStoreType',
              'type': 'string'},
             {'meaning': 'JSONObjectMeaning',
              'name': 'envSelection',
              'type': 'string'},
             {'meaning': 'DatetimeNoTz',
              'name': 'created_timestamp',
              'type': 'string'},
             {'meaning': 'Text',
              'name': 'last_modified_user',
              'type': 'string'},
             {'meaning': 'DatetimeNoTz',
              'name': 'last_modified_timestamp',
              'type': 'string'},
             {'meaning': 'JSONArrayMeaning', 'name': 'tags', 'type': 'string'},
             {'meaning': 'Text', 'name': 'created_by_user', 'type': 'string'},
             {'meaning': 'Text', 'name': 'managedFolderId', 'type': 'string'},
             {'meaning': 'Text', 'name': 'multimodalColumn', 'type': 'string'},
             {'meaning': 'Text', 'name': 'rebuildBehavior', 'type': 'string'},
             {'meaning': 'URL', 'name': 'url', 'type': 'string'}]}

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        """This never runs for anything that I can find."""
        return None

    def get_partitioning(self):
        """TBD"""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """TBD"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """TBD"""
        raise NotImplementedError
