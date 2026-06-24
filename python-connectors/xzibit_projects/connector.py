"""Connector that provides a dataset of all Projects on the DSS instance."""

from datetime import datetime

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import remove_prefix_from_keys, flatten_dict, get_dss_base_url


class ConnectorProjects(XzibitBaseConnector):
    """Connector that provides a dataset of all Projects on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, project_key):
        """Returns the DSS UI URL for the project, or None if inputs are missing."""
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
        for item_info in self.__client.list_projects():
            if records_limit > 0 and records_generated >= records_limit:
                return

            next_row = flatten_dict(item_info, include_keys=keys)
            next_row = remove_prefix_from_keys(next_row, "versionTag.")
            next_row["last_modified_timestamp"] = datetime.fromtimestamp(
                next_row.get("lastModifiedOn", 0) // 1000
            )
            next_row["url"] = self.get_url(next_row["projectKey"])
            records_generated += 1
            yield next_row

    def get_read_schema(self):
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
