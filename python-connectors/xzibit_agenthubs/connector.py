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

def get_agenthub_url(project_key, webapp_id): 
    base_url = get_dss_base_url()
    # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/Data_Dictionary_and_DSS_Instance_datasets_test_project/webapps/2NndNjZ_agent-hub/edit
    return f"{base_url}/projects/{project_key}/webapps/{webapp_id}_agent-hub/edit"

def is_agent_hub(webapp):
    webapp_type = webapp.get('type', '').lower()
    # Agent Hub is a plugin webapp. Its type usually follows the pattern 'plugin_id.webapp_id'
    # We check for the presence of specific keywords to identify it.
    if webapp_type == 'webapp_agent-hub_agent-hub':
        return True
    
    # Also check the name as a fallback if the user named it explicitly "Agent Hub" 
    # and the type is generic (less likely, but robust).
    if 'agent hub' in webapp.get('name', '').lower():
        return True
        
    return False


class ConnectorProjects(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
       #  print("[DEBUG agents] Constructor START")
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__count = 0

        
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        # 1. Retrieve list of all projects (User is Admin per assumptions)
        project_keys = self.__client.list_project_keys()

        agent_hubs = []

        for project_key in project_keys:
            try:
                project = self.__client.get_project(project_key)

                # List all webapps in the project
                webapps = project.list_webapps()

                for webapp in webapps:
                    if is_agent_hub(webapp):

                        pp(webapp)

                        # Collect relevant metadata
                        next_row = {
                            "projectKey": project_key,
                            "webapp_name": webapp.get("name",None),
                            "webapp_id": webapp.get("id",None),
                            "type": webapp.get("type"),
                            "created_by_user": webapp.get("createdBy", {}).get("login"),
                            "backendRunning": webapp.get("backendRunning",None),
                            "url": get_agenthub_url(project_key, webapp.get("id","")),
                            "created_on": datetime.fromtimestamp(webapp.get("createdOn", None) // 1000),
                            "lastModifiedBy": webapp.get("lastModifiedBy", {}).get('login',None),
                            "lastModifiedOn": datetime.fromtimestamp(webapp.get("lastModifiedOn", None) // 1000),
                            "tags": webapp.get("tags",[]),
                        }
                        self.__count += 1
                        yield next_row

            except Exception as e:
                print(f"Skipping project {project_key} due to error: {e}")

        

                
    def get_read_schema(self):
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText
        return None
            
####################################################################
# Same for all instances:
####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        # return len(self.objects_list)
        return self.__count

####################################################################
# Intentionally not implemented, not needed for this type
####################################################################
    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError
