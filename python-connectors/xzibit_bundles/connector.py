"""Connector that provides a dataset of all Bundles across every project."""

from dataiku import api_client
from dataiku.connector import Connector

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import get_dss_base_url, pp


class ConnectorBundles(XzibitBaseConnector, Connector):
    """Connector that provides a dataset of all Bundles across every project."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__object_name = "bundle"

    def get_url(self, project_key):
        """Returns the DSS UI URL for the project's bundles page, or None if inputs are missing.

        Bundle URLs require a trailing slash.  All bundles in a project share
        the same page; there is no per-bundle URL.
        """
        if any(v is None for v in (self.__baseurl, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/bundles-design/"

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

            # .list_exported_bundles() returns a dict, not a list
            obj_list = project_handle.list_exported_bundles().get("bundles", [])

            for obj_dict in obj_list:
                if records_limit > 0 and records_generated >= records_limit:
                    return
                try:
                    next_row = {
                        "project_key": project_key
                    }  # safe start in case exception happens.
                    next_row["url"] = self.get_url(project_key)

                    next_row["bundle_id"] = obj_dict.get("bundleId", "")
                    next_row["state"] = obj_dict.get("state", "")
                    next_row["type_badges"] = obj_dict.get("typeBadges", [])

                    next_row["exported_by_user"] = (
                        obj_dict.get("exportManifest", {})
                        .get("exportUserInfo", {})
                        .get("exportedBy", "")
                    )
                    next_row["exported_timestamp"] = (
                        obj_dict.get("exportManifest", {})
                        .get("exportUserInfo", {})
                        .get("exportedOn", "")
                    )
                    next_row["release_notes"] = (
                        obj_dict.get("exportManifest", {})
                        .get("exportUserInfo", {})
                        .get("releaseNotes", "")
                    )
                    next_row["projectStandardsSkipped"] = obj_dict.get(
                        "exportManifest", {}
                    ).get("projectStandardsSkipped", "")

                    next_row["generated_with_dss_version"] = obj_dict.get(
                        "exportManifest", {}
                    ).get("generatedWithDSSVersion", "")
                    next_row["exported_on_design_node_id"] = obj_dict.get(
                        "exportManifest", {}
                    ).get("designNodeId", "")

                    next_row["published_timestamp"] = obj_dict.get(
                        "publishedBundleState", {}
                    ).get("publishedOn", "")
                    next_row["published_by_user"] = obj_dict.get(
                        "publishedBundleState", {}
                    ).get("publishedBy", "")

                except Exception as e:
                    print(
                        f"[{self.__object_name}-generate_rows] [UNEXPECTED EXCEPTION] with {self.__object_name} in project {project_key}: {e}"
                    )

                finally:
                    records_generated += 1
                    yield next_row

    def get_read_schema(self):
        return None
