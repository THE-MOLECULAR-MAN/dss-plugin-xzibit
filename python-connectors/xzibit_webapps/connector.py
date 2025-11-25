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

def get_webapp_url(project_key, webapp_id): 
    base_url = get_dss_base_url()
    return f"{base_url}/projects/{project_key}/webapps/{webapp_id}/edit"


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
        for project_key in self.__client.list_project_keys():
            try:
                project = self.__client.get_project(project_key)
                # List all webapps in the project
                for webapp in project.list_webapps():
                    try:

                        pp(webapp)

                        # Collect relevant metadata
                        next_row = {
                            "projectKey": project_key,
                            #"webapp_name": webapp.get("name",None),
                            "webapp_id": webapp.get("id",None),
                            #"type": webapp.get("type"),
                            #"created_by_user": webapp.get("createdBy", {}).get("login"),
                            #"backendRunning": webapp.get("backendRunning",None),
                            #"url": get_agenthub_url(project_key, webapp.get("id","")),
                            #"created_on": datetime.fromtimestamp(webapp.get("createdOn", None) // 1000),
                            #"lastModifiedBy": webapp.get("lastModifiedBy", {}).get('login',None),
                            #"lastModifiedOn": datetime.fromtimestamp(webapp.get("lastModifiedOn", None) // 1000),
                            #"tags": webapp.get("tags",[]),
                        }
                        self.__count += 1
                        yield next_row
                    except Exception as e:
                        print(f"Skipping webapp {webapp} due to error: {e}")                        

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
