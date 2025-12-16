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

    def get_url(self, project_key, kb_id):
        """Create a URL to the object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, project_key, kb_id)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/knowledge-bank/{kb_id}/settings/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        for project_key in self.__client.list_project_keys():
            try:
                project = self.__client.get_project(project_key)
                # List all webapps in the project
                if records_limit > 0 and records_generated >= records_limit:
                    return
                for kb in project.list_knowledge_banks(as_type='objects'):
                    try:
                        if records_limit > 0 and records_generated >= records_limit:
                            return
                        settings_raw = kb.get_settings.get_raw()
                        
                        pp()
                      
                        next_row = {"projectKey": project_key}
                        next_row["kb_id"] = settings_raw.get('id', None)
                        next_row["kb_name"] = settings_raw.get('name', None)

                        next_row["url"] = self.get_url(project_key, webapp.get("id", ""))
#                         next_row["created_timestamp"] = datetime.fromtimestamp(webapp.get("createdOn", 0) // 1000)
#                         next_row["last_modified_user"] = webapp.get("lastModifiedBy", {}).get("login", None)
#                         next_row["last_modified_timestamp"] = datetime.fromtimestamp(webapp.get("lastModifiedOn", 0) // 1000)
#                         next_row["tags"] = webapp.get("tags", None)                        
#                         next_row["created_by_user"] = webapp.get("createdBy", {}).get("login", None)
                        
                    except Exception as e:
                        print(
                            f"[webapps-generate_rows] [UNEXPECTED WEBAPP EXCEPTION] {e} with webapp {next_row.get('webapp_name', None)}"
                        )
                    finally:
                        records_generated += 1
                        yield next_row                       

            except Exception as e:
                print(
                    f"[kb-generate_rows] [UNEXPECTED PROJECT EXCEPTION] {e} with project {project_key}"
                )
    

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText
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
