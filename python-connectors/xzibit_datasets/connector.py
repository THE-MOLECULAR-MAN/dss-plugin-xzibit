"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    safe_extract_dataset_metadata,
    get_dss_base_url,
)


class ConnectorDatasets(Connector):
    """TBD"""

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
        records_generated = 0

        # iterate through each object
        for pk in self.__client.list_project_keys():
            project_handle = self.__client.get_project(pk)

            # include_shared=True # introduced after 12.3.2
            for r in project_handle.list_datasets(as_type="objects"):
                if records_limit > 0 and records_generated >= records_limit:
                    return
                try:
                    dataset_handle = project_handle.get_dataset(r.id)
                    next_row = safe_extract_dataset_metadata(dataset_handle, pk)

                    next_row["url"] = self.get_url(r.id, pk)

                except Exception as e:
                    print(
                        f"[datasets-generate_rows] [UNEXPECTED EXCEPTION] with dataset {r.id} in project {pk}: {e}"
                    )
                    next_row = {"projectKey": pk, "name": r.id}
                finally:
                    records_generated += 1
                    yield next_row

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "id", "type": "string"},
                {"meaning": "Text", "name": "name", "type": "string"},
                {"meaning": "Text", "name": "projectKey", "type": "string"},
                {"meaning": "Text", "name": "type", "type": "string"},
                {"meaning": "FreeText", "name": "shortDesc", "type": "string"},
                {"meaning": "FreeText", "name": "description", "type": "string"},
                {"meaning": "Boolean", "name": "exists", "type": "boolean"},
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
                    "meaning": "Text",
                    "name": "creationTag.lastModifiedOn",
                    "type": "string",
                },
                {
                    "meaning": "Text",
                    "name": "versionTag.lastModifiedBy.login",
                    "type": "string",
                },
                {
                    "meaning": "Text",
                    "name": "versionTag.lastModifiedOn",
                    "type": "string",
                },
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
                # {"meaning": "JSONArrayMeaning", "name": "data_lineage", "type": "string"},
                # {"meaning": "LongMeaning", "name": "num_data_quality_rules", "type": "int"},
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
