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
        self.__keys = [
            "projectKey",
            "ownerLogin",
            "projectStatus",
            "contributors",
            "name",
            "shortDesc",
            "description",
            "tags",
            "versionTag.lastModifiedOn",
            "tutorialProject",
        ]
        self.__baseurl = get_dss_base_url()
        

    def get_project_url(self, project_key):
        # https://beta-design.se-platform.dataiku-sandbox.io/projects/Data_Dictionary_and_DSS_Instance_datasets_test_project/flow/
        try:
            if self.__baseurl is None or project_key is None:
                return None
            # trailing slash is MANDATORY
            return f"{self.__baseurl}/projects/{project_key}/flow/"
        except Exception: # yeah, I know this is bad practice
            return None


    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        # iterate through each object
        for item_info in self.__client.list_projects():
            # pp(item_info)
            next_row = flatten_dict(item_info, include_keys=self.__keys)

            # custom things for this specific class:
            next_row = remove_prefix_from_keys(next_row, "versionTag.")
            next_row["lastModifiedOn"] = datetime.fromtimestamp(
                next_row["lastModifiedOn"] // 1000
            )
            next_row['url_project'] = self.get_project_url(next_row['projectKey'])
            yield next_row

    def get_read_schema(self):
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText
        return {
            "columns": [
                {"name": "projectKey", "type": "string", "meaning": "Text"},
                {"name": "ownerLogin", "type": "string", "meaning": "Text"},
                {"name": "projectStatus", "type": "string", "meaning": "Text"},
                {
                    "name": "contributors",
                    "type": "string",
                    "meaning": "JSONArrayMeaning",
                },
                {"name": "name", "type": "string", "meaning": "Text"},
                {"name": "shortDesc", "type": "string", "meaning": "FreeText"},
                {"name": "description", "type": "string", "meaning": "FreeText"},
                {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
                {"name": "lastModifiedOn", "type": "datetimenotz", "meaning": "DatetimeNoTz"},
                {"name": "tutorialProject", "type": "boolean", "meaning": "Boolean"},
            ]
        }

    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.__client.list_projects())

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError
