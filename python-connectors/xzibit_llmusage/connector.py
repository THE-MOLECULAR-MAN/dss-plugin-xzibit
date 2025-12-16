"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import remove_prefix_from_keys, flatten_dict, get_dss_base_url, recursive_search_all, pp



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
                        next_row = {"projectKey": project_key, "dss_object_type": "agent"}
                        next_row['dss_object_id'] = agent_handle.id
                        agent_data = agent_handle.get_settings().get_raw()
                        next_row['dss_object_name'] = agent_data.get('name',None)
                        next_row['llmId'] = recursive_search_all(agent_data, "llmId")
                    except Exception as e:
                        print(f"[EXCEPTION] generate rows - llm Usage - agents: {e}")
                    finally:
                        if not next_row['llmId']:
                            # code agents don't have this field filled out. Code agents have to be manually inventoried and updated. boo.
                            continue
                        records_generated += 1
                        yield next_row
                        
                for agent_tool_handle in project.list_agent_tools(as_type='objects'):
                    try: 
                        next_row = {"projectKey": project_key, "dss_object_type": "agent tool"}
                        next_row['dss_object_id'] = agent_tool_handle.id
                        
                        data = agent_tool_handle.get_settings().get_raw()
                        pp(data)
                        next_row['dss_object_name'] = data.get('name', None)
                        # next_row['llmId'] =  data.get('params',{}).get('llmId', None)
                        next_row['llmId'] =  recursive_search_all(data, "llmId")
                    except Exception as e:
                        print(f"[EXCEPTION] generate rows - llm Usage - agent tool: {e}")
                    finally:
                        if not next_row['llmId']:
                            continue
                        records_generated += 1
                        yield next_row
                        
                        
                for kb_handle in project.list_agent_tools(as_type='objects'):
                    try: 
                        next_row = {"projectKey": project_key, "dss_object_type": "knowledge bank"}
                        next_row['dss_object_id'] = agent_tool_handle.id
                        
                        data = agent_tool_handle.get_settings().get_raw()
                        pp(data)
                        next_row['dss_object_name'] = data.get('name', None)
                        # next_row['llmId'] =  data.get('params',{}).get('llmId', None)
                        next_row['llmId'] =  recursive_search_all(data, "llmId")
                    except Exception as e:
                        print(f"[EXCEPTION] generate rows - llm Usage - agent tool: {e}")
                    finally:
                        if not next_row['llmId']:
                            continue
                        records_generated += 1
                        yield next_row
                
                
            except Exception as e:
                print(
                    f"[generate_rows] [UNEXPECTED EXCEPTION project] {e} with object"
                )

    

    def get_read_schema(self):
        """Returns the read schema for TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return None

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
