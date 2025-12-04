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
from xzibit.utils import get_dss_base_url, replace_empty_arrays_sets_with_none, pp


def get_agenttool_url(project_key, agenttool_id):
    """TBD"""
    base_url = get_dss_base_url()

    # at least one is None, return None
    if any(v is None for v in (agenttool_id, agenttool_id, project_key)):
        return None
    
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
        for project_key in self.__client.list_project_keys():
            # print(f"[generate_rows] Outer loop start on project key: {project_key}")
            try:
                project = self.__client.get_project(project_key)

                # List all agents in the current project

                for agent_item in project.list_agent_tools():
                    try:
                        next_row = {
                            "projectKey": project_key,
                        }

                        agent = project.get_agent_tool(agent_item.id)
                        settings = agent.get_settings() #  <dataikuapi.dss.agent_tool.DSSAgentToolSettings object>

                        # https://developer.dataiku.com/latest/api-reference/python/agents.html#dataikuapi.dss.agent_tool.DSSAgentToolSettings
                        raw_settings = settings.get_raw()

                        #print(f"[agent_tools.generate_rows] settings:")
                        #pp(raw_settings)
                        next_row["agent_tool_id"]   = raw_settings.get('id',None)
                        next_row["agent_tool_name"] = raw_settings.get('name',None)
                        next_row["agent_tool_type"] = raw_settings.get('type',None)
                        next_row["agent_tool_description_for_LLM"] = raw_settings.get('additionalDescriptionForLLM',None)
                        next_row["tags"] = replace_empty_arrays_sets_with_none(raw_settings.get("tags", []))
                        next_row["url"] =  get_agenttool_url(
                                project_key, next_row["agent_tool_id"]
                            )

                        creation_user = (
                                raw_settings.get("creationTag", {})
                                .get("lastModifiedBy", {})
                                .get("login", None)
                            )
                        next_row["creator_user"] = creation_user
                        
                        last_modified_on = datetime.fromtimestamp(
                            raw_settings.get("versionTag", {}).get("lastModifiedOn", None) // 1000
                        )
                        next_row["last_modified_timestamp"] = last_modified_on

                        last_modified_user = (
                            raw_settings.get("versionTag", {})
                            .get("lastModifiedBy", {})
                            .get("login", None)
                        )
                        next_row["last_modified_user"] = last_modified_user
                        next_row["agent_tool_params"] = replace_empty_arrays_sets_with_none(raw_settings.get('params',None))
                        next_row["agent_tool_LLMid"]  = raw_settings.get('params',{}).get("llmId", "")
                        
                        # next_row["customFields"]   = raw_settings.get('customFields',None)
                        next_row["dkuProperties"]    = replace_empty_arrays_sets_with_none(raw_settings.get('dkuProperties',None))
                        # next_row["checklists"]     = raw_settings.get('checklists',{}).get('checklists',None)
                        next_row["quickTestQuery"]   = replace_empty_arrays_sets_with_none(raw_settings.get('quickTestQuery',None))

                    # except (AttributeError, KeyError, TypeError, ValueError) as e:
                    except Exception as e:
                        print(
                            f"[agenttools.generaterows] [EXCEPTION] in {project_key}: {e}"
                        )
                    finally:
                        yield next_row

            except Exception as e_proj:
                # Pass on projects where we lack permissions or feature is disabled
                print(f"[generate_rows] Exception {project_key}: {e_proj}")


    def get_read_schema(self):
        """Returns the read schema for TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            'columns': [{'meaning': 'Text', 'name': 'projectKey', 'type': 'string',
             {'meaning': 'Text', 'name': 'agent_tool_id', 'type': 'string'},
             {'meaning': 'Text', 'name': 'agent_tool_name', 'type': 'string'},
             {'meaning': 'Text', 'name': 'agent_tool_type', 'type': 'string'},
             {'meaning': 'FreeText',
              'name': 'agent_tool_description_for_LLM',
              'type': 'string'},
             {'meaning': 'JSONArrayMeaning', 'name': 'tags', 'type': 'string'},
             {'meaning': 'URL', 'name': 'url', 'type': 'string'},
             {'meaning': 'Text', 'name': 'creator_user', 'type': 'string'},
             {'meaning': 'DatetimeNoTz',
              'name': 'last_modified_timestamp',
              'type': 'string'},
             {'meaning': 'Text',
              'name': 'last_modified_user',
              'type': 'string'},
             {'meaning': 'JSONObjectMeaning',
              'name': 'agent_tool_params',
              'type': 'string'},
             {'meaning': 'Text', 'name': 'agent_tool_LLMid', 'type': 'string'},
             {'meaning': 'Text', 'name': 'dkuProperties', 'type': 'string'},
             {'meaning': 'JSONObjectMeaning',
              'name': 'quickTestQuery',
              'type': 'string'}]}
                

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
