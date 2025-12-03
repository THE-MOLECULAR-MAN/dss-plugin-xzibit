####################################################################
# Unique imports for this Class
####################################################################
from datetime import datetime

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector

# from xzibit.utils import *
from xzibit.utils import get_dss_base_url, pp


def get_agenttool_url(project_key, agenttool_id):
    """TBD"""
    base_url = get_dss_base_url()
    # https://dev-design.se-platform.dataiku-sandbox.io/projects/Data_Dictionary_and_DSS_Instance_datasets_test_project/agent-tools/JfbcCw6
    return f"{base_url}/projects/{project_key}/agent-tools/{agenttool_id}"


class ConnectorProjects(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__objects_list = self.__client.list_project_keys()

    # pylint: disable=W0613
    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""

        # iterate through each object
        for project_key in self.__objects_list:
            # print(f"[generate_rows] Outer loop start on project key: {project_key}")
            try:
                project = self.__client.get_project(project_key)

                # List all agents in the current project

                for agent_item in project.list_agent_tools():
                    try:
                        # print(f"[generate_rows] Inner loop start on agent_item {agent_item}")
                        # Get the full agent object and its settings
                        # We need the full object to access .get_settings()
                        agent = project.get_agent_tool(agent_item.id)
                        settings = agent.get_settings() #  <dataikuapi.dss.agent_tool.DSSAgentToolSettings object at 0x7f90301a83d0>
                        # https://developer.dataiku.com/latest/api-reference/python/agents.html#dataikuapi.dss.agent_tool.DSSAgentToolSettings
                        # get_raw
                        raw_settings = settings.get_raw()
                        

                        print(f"[agent_tools.generate_rows] settings:")
                        pp(raw_settings))
                        # raw_settings = settings.get_raw()

                        next_row = {
                            "projectKey": project_key,
#                             "Agent Name": agent_item.name,
#                             "Agent ID": agent_item.id,
#                             "Created by user": creation_user,
#                             "Last modified by user": last_modified_user,
#                             "Active Version": active_version_id,
#                             # "LLM Model ID": llm_model_id,
#                             "LLM Vendor": llm_vendor,
#                             "LLM Connection Name": llm_connection_name,
#                             "LLM Model Name": llm_model,
#                             "Agent Type": agent_item.get("type", None),
#                             "Agent Version": agent_version,
#                             "tags": agent_item.get("tags", None),
#                             "Last Modified timestamp": last_modified_on,
                            # "Agent is active version": is_active_version,
#                             "Agent URL": get_agent_url(
#                                 project_key, agent_item.id, agent_version
#                             ),
                        }
                        yield next_row

                    except (AttributeError, KeyError, TypeError, ValueError) as e_agent:
                        # Print minimal error to avoid cluttering logs for expected data issues
                        print(
                            f"[generate_rows] [Skipping Agent] {agent_item.name} in {project_key}: {e_agent}"
                        )

            except Exception as e_proj:
                # Pass on projects where we lack permissions or feature is disabled
                print(f"[generate_rows] Exception {project_key}: {e_proj}")

        # print("[generate_rows] END")

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "Created by user", "type": "string"},
                {"meaning": "Text", "name": "Last modified by user", "type": "string"},
                {"meaning": "JSONArrayMeaning", "name": "tags", "type": "string"},
                {
                    "meaning": "DatetimeNoTz",
                    "name": "Last Modified timestamp",
                    "type": "datetimenotz",
                },
                {"meaning": "Text", "name": "projectKey", "type": "string"},
                {"meaning": "Text", "name": "Agent Name", "type": "string"},
                {"meaning": "Text", "name": "Agent ID", "type": "string"},
                {"meaning": "Text", "name": "Active Version", "type": "string"},
                {"meaning": "Text", "name": "LLM Vendor", "type": "string"},
                {"meaning": "Text", "name": "LLM Connection Name", "type": "string"},
                {"meaning": "Text", "name": "LLM Model Name", "type": "string"},
                {"meaning": "Text", "name": "Agent Type", "type": "string"},
                {"meaning": "Text", "name": "Agent Version", "type": "string"},
                {"meaning": "URL", "name": "Agent URL", "type": "string"},
            ],
        }

    ####################################################################
    # Same for all instances:
    ####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        """TBD"""
        return None

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
    def get_partitioning(self):
        """TBD"""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """TBD"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """TBD"""
        raise NotImplementedError
