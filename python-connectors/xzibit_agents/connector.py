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
        #self.__unique_id_key_name = 'projectKey'
        #self.__keys   = [self.unique_id_key_name, 'ownerLogin', 'projectStatus', 'contributors', 'name', 
            'shortDesc', 'description',
            'tags', 'versionTag.lastModifiedOn', 'tutorialProject']
        self.__objects_list = self.__client.list_project_keys()


        
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):       
        
         # iterate through each object
        for project_key in self.__objects_list:
            try:
                project = client.get_project(project_key)

                # List all agents in the current project
                # Note: This returns a list of DSSAgentListItem objects
                agents = project.list_agents()

                for agent_item in agents:
                    try:
                        # Get the full agent object and its settings
                        # We need the full object to access .get_settings()
                        agent = project.get_agent(agent_item.id)
                        settings = agent.get_settings()
                        raw_settings = settings.get_raw()

                        # We typically want the LLM used by the *Active* version of the agent
                        active_version_id = settings.active_version

                        llm_model_id = "N/A"

                        if active_version_id:
                            # Retrieve settings for the active version
                            version_settings = settings.get_version_settings(active_version_id)

                            # 1. Try standard Visual Agent property
                            try:
                                llm_model_id = version_settings.llm_id
                            except AttributeError:
                                # 2. Fallback: Check raw settings (common for Code Agents)
                                ver_raw = version_settings.get_raw()
                                llm_model_id = ver_raw.get("llmId", None)

                                # Sometimes stored under 'generation' block for complex setups
                                if not llm_model_id and "generation" in ver_raw:
                                    llm_model_id = ver_raw["generation"].get("llmId")

                        # Append to our dataset list
                        # FIX: Use raw_settings (from the full object) instead of agent_item.get_raw()
                        pp(raw_settings)
                        creation_user = raw_settings.get("creationTag", {}).get("user", "Unknown")
                        
                        next_row = {
                            "Project Key": project_key,
                            "Agent Name": agent_item.name,
                            "Agent ID": agent_item.id,
                            "Created By": creation_user, 
                            "Active Version": active_version_id,
                            "LLM Model ID": llm_model_id
                        }
                        yield next_row

                    except Exception as e_agent:
                        # Print minimal error to avoid cluttering logs
                        print(f"  [Skipping Agent] {agent_item.name} in {project_key}: {e_agent}")

            except Exception as e_proj:
                # Pass on projects where we lack permissions or feature is disabled
                pass


                
                
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
