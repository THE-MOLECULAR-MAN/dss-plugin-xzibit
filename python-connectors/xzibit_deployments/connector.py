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


class ConnectorAPIServices(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__object_name = "Deployment"

    # Bundles do not have a unique page, just a shared page like meanings
#     def get_url(self, id, project_key):
#         """Create a URL to the DSS object in question in this specific DSS instance.
#         Return None if any of the inputs are None."""
#         # https://honker-design-2.se-platform.dataiku-sandbox.io/projects/DKU_TSHIRTS/api-designer/prediciton_model_API_service/endpoints/
#         # does need a trailing slash
#         # at least one is None, return None
#         if any(v is None for v in (self.__baseurl, id, project_key)):
#             return None
#         return f"{self.__baseurl}/projects/{project_key}/api-designer/{id}/endpoints/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        
        deployer = self.__client.get_projectdeployer()
        
        deployments = deployer.list_deployments(as_objects=True)

        # iterate through each object
        for deployment_handle in deployments:

            project_handle = self.__client.get_project(project_key)

            # iterate through each object in the project
            for obj_handle in project_handle.list_api_services(as_type="objects"):
                # https://developer.dataiku.com/latest/api-reference/python/api-designer.html#dataikuapi.dss.apiservice.DSSAPIService

                if records_limit > 0 and records_generated >= records_limit:
                    return
                try:
                    next_row = {"project_key": project_key} # safe start in case exception happens.
                    
                    obj_id =  obj_handle.id
                    next_row["api_service_id"] = obj_id
                    
                    # next_row["url"] = self.get_url(obj_id, project_key)


                    
                except Exception as e:
                    print(
                        f"[{self.__object_name}-generate_rows] [UNEXPECTED EXCEPTION] in project {project_key}: {e}"
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
