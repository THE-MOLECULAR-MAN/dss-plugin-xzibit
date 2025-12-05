####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    safe_extract_dataset_metadata,
    get_dss_base_url,
)


def get_dataset_url(project_key, dataset_id):
    # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/PMMOPTIMIZINGOMNICHANNELMARKETINGLLM/datasets/Sales_Marketing_queries/explore/
    try:
        base_url = get_dss_base_url()
        if base_url is None or project_key is None or dataset_id is None:
            return None
        # trailing slash is MANDATORY
        return f"{base_url}/projects/{project_key}/datasets/{dataset_id}/explore/"
    except Exception:  # yeah, I know this is bad practice
        return None


class ConnectorDatasets(Connector):
    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, id, project_key):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/datasets/{id}/explore/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""

        # iterate through each object
        for pk in self.__client.list_project_keys():
            project_handle = self.__client.get_project(pk)

            for r in project_handle.list_datasets(
                as_type="objects", include_shared=True
            ):
                try:
                    #                    num_rows += 1
                    dataset_handle = project_handle.get_dataset(r.id)
                    next_row = safe_extract_dataset_metadata(dataset_handle, pk)
                    # next_row["dataset_url"] = get_dataset_url(pk, r.id)
                    next_row["url"] = self.get_url(r.id, pk)
                    yield next_row

                except Exception as e:
                    print(
                        f"GENERIC EXCEPTION in xzibit_datasets/connector.py - generate_rows with dataset {r.id} in project {pk}: {e} "
                    )
                    # r is of type "dataikuapi.dss.dataset.DSSDataset"
                    # Test failed: com.dataiku.dip.server.controllers.NotFoundException: dataset does not exist:
                    yield {"projectKey": pk, "name": r.id}

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "projectKey", "type": "string"},
                {"meaning": "Text", "name": "id", "type": "string"},
                {"meaning": "Text", "name": "name", "type": "string"},
                {"meaning": "Boolean", "name": "exists", "type": "boolean"},
                {"meaning": "Text", "name": "type", "type": "string"},
                {"meaning": "Text", "name": "formatType", "type": "string"},
                {"meaning": "Text", "name": "params.connection", "type": "string"},
                {"meaning": "Boolean", "name": "managed", "type": "boolean"},
                {"meaning": "Text", "name": "params.mode", "type": "string"},
                {"meaning": "Text", "name": "params.table", "type": "string"},
                {"meaning": "Text", "name": "params.schema", "type": "string"},
                {"meaning": "Text", "name": "params.path", "type": "string"},
                {
                    "meaning": "Text",
                    "name": "creationTag.lastModifiedBy.login",
                    "type": "string",
                },
                {
                    "meaning": "DatetimeNoTz",
                    "name": "creationTag.lastModifiedOn",
                    "type": "datetimenotz",
                },
                {
                    "meaning": "Text",
                    "name": "versionTag.lastModifiedBy.login",
                    "type": "string",
                },
                {
                    "meaning": "DatetimeNoTz",
                    "name": "versionTag.lastModifiedOn",
                    "type": "datetimenotz",
                },
                {"meaning": "FreeText", "name": "shortDesc", "type": "string"},
                {"meaning": "FreeText", "name": "description", "type": "string"},
                {
                    "meaning": "Text",
                    "name": "params.metastoreDatabaseName",
                    "type": "string",
                },
                {"meaning": "Text", "name": "params.folderSmartId", "type": "string"},
                {"meaning": "JSONArrayMeaning", "name": "tags", "type": "string"},
                {"meaning": "Boolean", "name": "featureGroup", "type": "boolean"},
                {"meaning": "LongMeaning", "name": "num_metrics_checks", "type": "int"},
                {"meaning": "LongMeaning", "name": "num_columns", "type": "int"},
                {
                    "meaning": "JSONArrayMeaning",
                    "name": "column_names",
                    "type": "string",
                },
                {"meaning": "URL", "name": "url", "type": "string"},
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
