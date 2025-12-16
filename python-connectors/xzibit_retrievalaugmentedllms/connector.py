"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import remove_prefix_from_keys, flatten_dict, get_dss_base_url, jd

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

    def get_url(self, id, project_key, name):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, id, project_key, name)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/savedmodels/{id}/retrieval-augmented-llm/S-{project_key}-{id}-{name}"
    
    def get_current_version_info(self, current_version_id, version_info):
        """TODO: Does this handle cases where current_version_id or version_info are None or {}?"""
        try:
            res = {}
            for v in version_info:
                if v.get("versionId", None) == current_version_id:
                    res = v
        except Exception as e:
            print(f"Exception in retrievalaugmentedllms.get_current_version_info: {e}")
        finally:
            return res
    
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

                # iterate through Knowledge Banks in this project as objects
                # intentionally not using 'list' since that can be slower for some object types
                for obj_handle in project.list_retrieval_augmented_llms(as_type='objects'):
                    try:

                        # exit early if exceeded the number of records requested
                        if records_limit > 0 and records_generated >= records_limit:
                            return

                        # initializing first column in case their is an exception, the yield will still work
                        next_row = {"projectKey": project_key}
                        
                        # fetch settings as dict
                        settings_raw = obj_handle.get_settings().get_raw()
                        
                        # display debug info during development
                        # for example, finding key names when adding new columns to dataset
                        jd(settings_raw)
                        
                        # add features that are unique to this object type
                        next_row["id"] = settings_raw.get('id', None)
                        next_row["activeVersion"] = settings_raw.get('activeVersion', None)
                        
                        versions = settings_raw.get('versions', {})
                        current_version_info_raw = self.get_current_version_info(next_row["activeVersion"], versions)
                        
                        
                        
                        # next_row["name"] = settings_raw.get('name', None)
                        

                        # URL is fetched using class method that specifically implements this DSS object type:
                        # next_row["url"] = self.get_url(next_row["id"], project_key, next_row["name"])
                        
                        # add features that are almost always the same for different DSS object types
                        next_row["created_timestamp"] = datetime.fromtimestamp(current_version_info_raw.get("creationTag",{}).get("lastModifiedOn", 0) // 1000)
                        next_row["last_modified_user"] = current_version_info_raw.get("versionTag",{}).get("lastModifiedBy", {}).get("login", None)
                        next_row["last_modified_timestamp"] = datetime.fromtimestamp(current_version_info_raw.get("versionTag",{}).get("lastModifiedOn", 0) // 1000)
                        next_row["tags"] = settings_raw.get("tags", None)
                        next_row["created_by_user"] = current_version_info_raw.get("creationTag",{}).get("lastModifiedBy", {}).get("login", None)                        
                        
                    except Exception as e:
                        print(
                            f"[generate_rows] [UNEXPECTED EXCEPTION] {e} with object"
                        )
                    finally:
                        # return something, even if an exception occurred.
                        records_generated += 1
                        yield next_row                       

            except Exception as e:
                print(
                    f"[generate_rows] [UNEXPECTED PROJECT EXCEPTION] {e} with project {project_key}"
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
