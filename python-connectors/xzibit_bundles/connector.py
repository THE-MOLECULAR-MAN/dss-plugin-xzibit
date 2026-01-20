"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    get_dss_base_url,
    pp
)


class ConnectorBundles(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__object_name = "bundle"

    # Bundles do not have a unique page, just a shared page like meanings
    def get_url(self, project_key):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/DKU_TSHIRTS/bundles-design/
        # does need a trailing slash
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, id, project_key)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/bundles-design/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        print(f"[{self.__object_name}-generate_rows] START")

        # iterate through each object
        for project_key in self.__client.list_project_keys():
            print(f"[{self.__object_name}-generate_rows] PROJECT KEY: {project_key}")
            project_handle = self.__client.get_project(project_key)

            # .list_exported_bundles() returns a dict, not a list
            x = project_handle.list_exported_bundles().get["bundles",[]]
            pp(x)
            for key, value in x.items():
                #print(f"[{self.__object_name}-generate_rows] dict key {key}")
                if records_limit > 0 and records_generated >= records_limit:
                    return
                try:
                    next_row = {"projectKey": project_key} # safe start in case exception happens.
                    next_row["url"] = self.get_url(project_key)
                    next_row["bundleId"] = value.get("bundleId","")
                    next_row["bundle_creator"] = value.get("requester","")
                    next_row["created"] = value.get("startTime","")
                    #print("=== DEBUG INFO ===")
                    #print(key)
                    #print(value)
                    #pp({key: value})

                except Exception as e:
                    print(
                        f"[{self.__object_name}-generate_rows] [UNEXPECTED EXCEPTION] with {self.__object_name} in project {project_key}: {e}"
                    )
                    
                finally:
                    records_generated += 1
                    yield next_row

        print(f"[{self.__object_name}-generate_rows] END")

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
