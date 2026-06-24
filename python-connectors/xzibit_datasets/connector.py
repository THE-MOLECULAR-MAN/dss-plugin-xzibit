"""Connector that provides a dataset of all Datasets across every project."""

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import (
    safe_extract_dataset_metadata,
    get_dss_base_url,
)


class ConnectorDatasets(XzibitBaseConnector):
    """Connector that provides a dataset of all Datasets across every project."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, dataset_id, project_key):
        """Returns the DSS UI URL for the dataset, or None if inputs are missing."""
        if any(v is None for v in (self.__baseurl, dataset_id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/datasets/{dataset_id}/explore/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        records_generated = 0

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
        return None
