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

                # exit early if exceeded the number of records requested
                if records_limit > 0 and records_generated >= records_limit:
                    return

                for kb in project.list_knowledge_banks(as_type='objects'):
                    try:

                        # exit early if exceeded the number of records requested                        
                        if records_limit > 0 and records_generated >= records_limit:
                            return

                        # initializing first column in case their is an exception, the yield will still work
                        next_row = {"projectKey": project_key}
                        
                        # fetch settings as dict
                        settings_raw = kb.get_settings().get_raw()
                        
                        # display debug info during development, adding features:
                        # pp(settings_raw)
                        
                        # add features that are unique to this object type
                        next_row["kb_id"] = settings_raw.get('id', None)
                        next_row["kb_name"] = settings_raw.get('name', None)
                        next_row["kb_embeddingLLMId"] = settings_raw.get('embeddingLLMId', None)
                        next_row["retrieverType"] = settings_raw.get('retrieverType', None)
                        next_row["kb_vectorStoreType"] = settings_raw.get('vectorStoreType', None)
                        next_row["envSelection"] = settings_raw.get('envSelection', None)
                        next_row["managedFolderId"] = settings_raw.get("managedFolderId", None)
                        next_row["multimodalColumn"] = settings_raw.get("multimodalColumn", None)
                        next_row["rebuildBehavior"] = settings_raw.get("rebuildBehavior", None)

                        # URL is fetched using class method that specifically implements this DSS object type:
                        next_row["url"] = self.get_url(project_key, next_row["kb_id"])
                        
                        # add features that are almost always the same for different DSS object types
                        next_row["created_timestamp"] = datetime.fromtimestamp(settings_raw.get("creationTag",{}).get("lastModifiedOn", 0) // 1000)
                        next_row["last_modified_user"] = settings_raw.get("versionTag",{}).get("lastModifiedBy", {}).get("login", None)
                        next_row["last_modified_timestamp"] = datetime.fromtimestamp(settings_raw.get("versionTag",{}).get("lastModifiedOn", 0) // 1000)
                        next_row["tags"] = settings_raw.get("tags", None)
                        next_row["created_by_user"] = settings_raw.get("creationTag",{}).get("lastModifiedBy", {}).get("login", None)                        
                        
                    except Exception as e:
                        print(
                            f"[generate_rows] [UNEXPECTED EXCEPTION] {e} with object"
                        )
                    finally:
                        records_generated += 1
                        yield next_row                       

            except Exception as e:
                print(
                    f"[kb-generate_rows] [UNEXPECTED PROJECT EXCEPTION] {e} with project {project_key}"
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
