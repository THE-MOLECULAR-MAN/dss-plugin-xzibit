"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_dss_base_url, pp


class ConnectorDeployments(Connector):
    """Connector that provides a dataset of all project deployer deployments."""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__object_name = "Deployment"

    # Bundles do not have a unique page, just a shared page like meanings
    def get_url(self, bundle_id, project_key):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # https://honker-design-2.se-platform.dataiku-sandbox.io/project-deployer/projects/DSS_Data_Plugin_Test/bundle/example_bundle1/
        # does need a trailing slash
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, bundle_id, project_key)):
            return ""
        return f"{self.__baseurl}/project-deployer/projects/{project_key}/bundle/{bundle_id}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0

        try:
            deployer = self.__client.get_projectdeployer()
        except Exception:
            print("No deployers connected")
            return None

        # iterate through each object
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
        """TBD"""
        return None

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
