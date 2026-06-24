"""Connector that provides a dataset of all Apps on the DSS instance."""

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import flatten_dict, get_dss_base_url


class ConnectorApps(XzibitBaseConnector):
    """Connector that provides a dataset of all Apps on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, app_id):
        """Returns the DSS UI URL for the app, or None if inputs are missing.

        Apps must NOT have a trailing slash.
        """
        if any(v is None for v in (self.__baseurl, app_id)):
            return None
        return f"{self.__baseurl}/apps/{app_id}"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        records_generated = 0
        keys = [
            "appId",
            "appVersion",
            "label",
            "origin",
            "shortDesc",
            "tags",
            "isAppImg",
            "instanceCount",
            "useAsRecipe",
            "onlyLimitedVisibility",
        ]
        for item_info in self.__client.list_apps():
            if records_limit > 0 and records_generated >= records_limit:
                return

            next_row = flatten_dict(item_info, include_keys=keys)
            next_row["url"] = self.get_url(next_row["appId"])
            records_generated += 1
            yield next_row

    def get_read_schema(self):
        return {
            "columns": [
                {
                    "meaning": "Boolean",
                    "name": "onlyLimitedVisibility",
                    "type": "boolean",
                },
                {"meaning": "Text", "name": "appId", "type": "string"},
                {"meaning": "Text", "name": "appVersion", "type": "string"},
                {"meaning": "Text", "name": "label", "type": "string"},
                {"meaning": "FreeText", "name": "shortDesc", "type": "string"},
                {"meaning": "JSONArrayMeaning", "name": "tags", "type": "string"},
                {"meaning": "Boolean", "name": "isAppImg", "type": "boolean"},
                {"meaning": "Text", "name": "origin", "type": "string"},
                {"meaning": "Text", "name": "originProjectKey", "type": "string"},
                {"meaning": "LongMeaning", "name": "instanceCount", "type": "int"},
                {"meaning": "Boolean", "name": "useAsRecipe", "type": "boolean"},
                {"meaning": "URL", "name": "url", "type": "string"},
            ]
        }
