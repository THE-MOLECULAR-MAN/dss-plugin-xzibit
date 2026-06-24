"""Connector that provides a dataset of all API Services across every project."""

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import get_dss_base_url, pp


class ConnectorAPIServices(XzibitBaseConnector):
    """Connector that provides a dataset of all API Services across every project."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__object_name = "API Service"

    def get_url(self, service_id, project_key):
        """Returns the DSS UI URL for the API service, or None if inputs are missing.

        API service URLs require a trailing slash.
        """
        if any(v is None for v in (self.__baseurl, service_id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/api-designer/{service_id}/endpoints/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        records_generated = 0

        for project_key in self.__client.list_project_keys():
            project_handle = self.__client.get_project(project_key)

            for obj_handle in project_handle.list_api_services(as_type="objects"):
                if records_limit > 0 and records_generated >= records_limit:
                    return
                try:
                    next_row = {
                        "project_key": project_key
                    }  # safe start in case exception happens.

                    obj_id = obj_handle.id
                    next_row["api_service_id"] = obj_id

                    next_row["url"] = self.get_url(obj_id, project_key)

                    next_row["num_versions"] = len(obj_handle.list_packages())

                    obj_settings = obj_handle.get_settings().get_raw()
                    next_row["last_modified_timestamp"] = obj_settings.get(
                        "versionTag", {}
                    ).get("lastModifiedOn", "")
                    next_row["last_modified_by_user"] = (
                        obj_settings.get("versionTag", {})
                        .get("lastModifiedBy", {})
                        .get("login", "")
                    )
                    next_row["num_endpoints"] = len(obj_settings.get("endpoints", []))
                    next_row["tags"] = obj_settings.get("tags", [])
                    next_row["name"] = obj_settings.get("name", "")

                except Exception as e:
                    print(
                        f"[{self.__object_name}-generate_rows] [UNEXPECTED EXCEPTION] in project {project_key}: {e}"
                    )

                finally:
                    records_generated += 1
                    yield next_row

    def get_read_schema(self):
        return None
