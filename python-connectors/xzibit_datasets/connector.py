####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *

def get_dataset_url():
    # https://beta-design.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/brave-mcp/
    # https://honker-design-2.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/CausalModels
    # https://honker-design-2.se-platform.dataiku-sandbox.io/admin/code-envs/design/python/CausalModels/
    try:
        base_url = get_dss_base_url()
        if base_url is None or env_lang is None or base_url is None:
            return None
        # trailing slash is MANDATORY
        return f"{base_url}/admin/code-envs/design/{env_lang.lower()}/{env_name}/"
    except Exception: # yeah, I know this is bad practice
        return None


class ConnectorDatasets(Connector):
    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)

        self.__client = api_client()
        self.__objects_list = {}
        #         self.__keys = ['projectKey', 'name', 'type', 'formatType', 'params.connection',
        #                        'managed', 'params.mode', 'params.table', 'params.schema', 'params.database',
        #                        'params.path',
        #                        'creationTag.lastModifiedBy.login', 'creationTag.lastModifiedOn',
        #                        'versionTag.lastModifiedBy.login',  'versionTag.lastModifiedOn',
        #                        'shortDesc', 'description', 'params.metastoreDatabaseName',
        #                        'params.folderSmartId', 'tags', 'featureGroup',
        #                       ]
        self.__count = 0

        for pk in self.__client.list_project_keys():
            project_handle = self.__client.get_project(pk)
            self.__objects_list[pk] = project_handle.list_datasets(
                as_type="objects", include_shared=True
            )
            self.__count += len(self.__objects_list[pk])

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""

        # key_mapping = set()
        # num_rows = 0

        # iterate through each object
        for pk, proj_datasets in self.__objects_list.items():
            project_handle = self.__client.get_project(pk)

            for r in proj_datasets:
                try:
                    #                    num_rows += 1
                    dataset_handle = project_handle.get_dataset(r.id)
                    next_row = safe_extract_dataset_metadata(dataset_handle, pk)
                    yield next_row

                except Exception as e:
                    print(
                        f"GENERIC EXCEPTION in xzibit_datasets/connector.py - generate_rows with dataset {r.id} in project {pk}: {e} "
                    )
                    # r is of type "dataikuapi.dss.dataset.DSSDataset"
                    # Test failed: com.dataiku.dip.server.controllers.NotFoundException: dataset does not exist:
                    yield {"projectKey": pk, "name": r.id}

    ####################################################################
    # Same for all instances:
    ####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        """TBD"""
        return self.__count

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
    def get_partitioning(self):
        """TBD"""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """TBD"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """TBD"""
        raise NotImplementedError

    def get_read_schema(self):
        """TBD"""
        return None
