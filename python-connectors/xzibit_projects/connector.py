"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector


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

    def get_url(self, project_key):
        """Create a URL to the object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/flow/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        keys = [
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
        # iterate through each object
        for item_info in self.__client.list_projects():
            if records_limit > 0 and records_generated >= records_limit:
                return

            next_row = flatten_dict(item_info, include_keys=keys)

            # custom things for this specific class:
            next_row = remove_prefix_from_keys(next_row, "versionTag.")
            next_row["last_modified_timestamp"] = datetime.fromtimestamp(
                next_row.get("lastModifiedOn", 0) // 1000
            )
            next_row["url"] = self.get_url(next_row["projectKey"])
            records_generated += 1
            yield next_row

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {"name": "projectKey", "type": "string", "meaning": "Text"},
                {"name": "name", "type": "string", "meaning": "Text"},
                {"name": "shortDesc", "type": "string", "meaning": "FreeText"},
                {"name": "description", "type": "string", "meaning": "FreeText"},
                {"name": "ownerLogin", "type": "string", "meaning": "Text"},
                {"name": "projectStatus", "type": "string", "meaning": "Text"},
                {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
                {
                    "name": "last_modified_timestamp",
                    "type": "string",
                    "meaning": "Text",
                },
                {"name": "tutorialProject", "type": "boolean", "meaning": "Boolean"},
                {
                    "name": "contributors",
                    "type": "string",
                    "meaning": "JSONArrayMeaning",
                },
                {"name": "url", "type": "string", "meaning": "URL"},
            ]
        }

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
