
####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *

####################################################################
# Unique imports for this Class
####################################################################
import re
from datetime import datetime

def convert_webapp_name_to_url_name(web_app_name):
    """x"""
    # Connect_Name-Test1234567890-=_+!@#$%^&*(),.<>/?'"[{]}\|
    # becomes
    # connectname-test1234567890-


def make_url_friendly(text):
    """
    Converts a string to a Dataiku URL-friendly format:
    1. Converts to lower case
    2. Removes all characters except alphanumeric (letters/numbers) and spaces
    3. Replaces spaces (and runs of spaces) with a single hyphen
    """
    if not isinstance(text, str):
        return str(text)
    
    # 1. Convert to lower case
    text = text.lower()
    
    # 2. Keep only alphanumeric characters and spaces
    # regex explanation: [^a-z0-9\s] matches anything that is NOT a lowercase letter, number, or whitespace
    text = re.sub(r'[^a-z0-9\s]', '', text)
    
    # 3. Replace one or more whitespace characters with a single hyphen
    text = re.sub(r'\s+', '-', text)
    
    # (Optional) Strip leading/trailing hyphens if spaces were at the ends
    # text = text.strip('-')
    
    return text

def get_webapp_url(project_key, webapp_id, web_app_name): 
    base_url = get_dss_base_url()
    # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/Data_Dictionary_and_DSS_Instance_datasets_test_project/webapps/gY7nbkW_connectname-test1234567890-/edit
    # Connect_Name-Test1234567890-=_+!@#$%^&*(),.<>/?'"[{]}\|
    safe_name = make_url_friendly(web_app_name)
    return f"{base_url}/projects/{project_key}/webapps/{webapp_id}_{safe_name}/edit"


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
                        # pp(webapp)

                        # Collect relevant metadata
                        next_row = {
                            "projectKey": project_key,
                            "webapp_name": webapp.get("name",None),
                            "webapp_id": webapp.get("id",None),
                            "type": webapp.get("type"),
                            "created_by_user": webapp.get("createdBy", {}).get("login"),
                            "backendRunning": webapp.get("backendRunning",None),
                            "url": get_webapp_url(project_key, webapp.get("id",""),webapp.get("name","")),
                            "created_on": datetime.fromtimestamp(webapp.get("createdOn", None) // 1000),
                            "lastModifiedBy": webapp.get("lastModifiedBy", {}).get('login',None),
                            "lastModifiedOn": datetime.fromtimestamp(webapp.get("lastModifiedOn", None) // 1000),
                            "tags": webapp.get("tags",[]),
                            "is_code_webapp": webapp.get("type") in ['SHINY', 'STANDARD', 'BOKEH', 'DASH'],
                        }
                        self.__count += 1
                        yield next_row
                    except Exception as e:
                        print(f"Skipping webapp due to error: {e}")                        

            except Exception as e:
                print(f"Skipping project {project_key} due to error: {e}")

        

                
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
                    "name":    "webapp_name", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "webapp_id", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "type", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "created_by_user", 
                    "type":    "string",
                    "meaning": "Text"
                },
                {
                    "name":    "backendRunning", 
                    "type":    "boolean",
                    "meaning": "Boolean"
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
