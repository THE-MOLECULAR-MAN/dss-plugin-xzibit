"""Connector that provides a dataset of all project deployer deployments."""

from dataiku import api_client
from dataiku.connector import Connector

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import get_dss_base_url, pp


class ConnectorDeployments(XzibitBaseConnector, Connector):
    """Connector that provides a dataset of all project deployer deployments."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__object_name = "Deployment"

    def get_url(self, bundle_id, project_key):
        """Returns the DSS UI URL for the deployment, or None if inputs are missing.

        Deployment URLs require a trailing slash.
        """
        if any(v is None for v in (self.__baseurl, bundle_id, project_key)):
            return None
        return f"{self.__baseurl}/project-deployer/projects/{project_key}/bundle/{bundle_id}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        records_generated = 0

        try:
            deployer = self.__client.get_projectdeployer()
        except Exception:
            print("No deployers connected")
            return None

        for deployment_handle in deployer.list_deployments(as_objects=True):
            if records_limit > 0 and records_generated >= records_limit:
                return
            try:
                next_row = {"deployment_id": "EXCEPTION"}  # just in case
                next_row["deployment_id"] = deployment_handle.id

                obj_settings = deployment_handle.get_settings().get_raw()

                next_row["bundle_id"] = obj_settings.get("bundleId", "")
                next_row["project_key"] = obj_settings.get("deployedProjectKey", "")
                next_row["infrastructure_id"] = obj_settings.get("infraId", "")
                next_row["deployment_type"] = obj_settings.get("type", "")
                next_row["last_modified_timestamp"] = obj_settings.get(
                    "versionTag", {}
                ).get("lastModifiedOn", "")
                next_row["last_modified_user"] = (
                    obj_settings.get("versionTag", {})
                    .get("lastModifiedBy", {})
                    .get("login", "")
                )

                next_row["health"] = deployment_handle.get_status().get_health()
                next_row["highest_governance_severity"] = str(
                    deployment_handle.get_governance_status().get("maxSeverity", "")
                )

                next_row["url"] = self.get_url(
                    next_row["bundle_id"], next_row["project_key"]
                )
            except Exception as e:
                print(
                    f"[{self.__object_name}-generate_rows] [UNEXPECTED EXCEPTION]: {e}"
                )
            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        return None
