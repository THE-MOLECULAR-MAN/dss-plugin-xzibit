####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *


####################################################################
# Unique imports for this Class
####################################################################
# none.

class ConnectorDatasets(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        
        self.__client = api_client()        
        self.__objects_list = {}
        self.__keys = ['projectKey', 'name', 'type', 'formatType', 'params.connection',
                       'managed', 'params.mode', 'params.table', 'params.schema', 'params.database',
                       'params.path', 
                       'creationTag.lastModifiedBy.login', 'creationTag.lastModifiedOn',
                       'versionTag.lastModifiedBy.login',  'versionTag.lastModifiedOn',
                       'shortDesc', 'description', 'params.metastoreDatabaseName',
                       'params.folderSmartId', 'tags', 'featureGroup',
                      ]
        self.__count = 0

        for pk in self.__client.list_project_keys():
            project_handle = self.__client.get_project(pk)
            self.__objects_list[pk] = project_handle.list_datasets(as_type='objects', include_shared=True)
            self.__count += len(self.__objects_list[pk])


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # key_mapping = set()
        # num_rows = 0
        
         # iterate through each object
        for pk, proj_datasets in self.__objects_list.items():
            project_handle = self.__client.get_project(pk)

            for r in proj_datasets:
                try:
#                    num_rows += 1
                    dataset_handle = project_handle.get_dataset(r.id)
                    next_row = safe_extract_dataset_metadata(dataset_handle)
                    yield next_row

                except Exception as e:
                    print(f"GENERIC EXCEPTION in xzibit_datasets/connector.py - generate_rows with dataset {r.id} in project {pk}: {e} ")
                    # r is of type "dataikuapi.dss.dataset.DSSDataset"
                    # Test failed: com.dataiku.dip.server.controllers.NotFoundException: dataset does not exist:
                    yield {'projectKey': pk,
                               'name':       r.id,
                               'dataset_exists': 'EXCEPTION'
                              }
            
####################################################################
# Same for all instances:
####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.__objects_list)

####################################################################
# Intentionally not implemented, not needed for this type
####################################################################
    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError

    def get_read_schema(self):
        return None
