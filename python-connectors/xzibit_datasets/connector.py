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
        
        self.client = api_client()        
        self.objects_list = {}
        self.__keys = ['projectKey', 'name', 'type', 'formatType', 'params.connection',
                       'managed', 'params.mode', 'params.table', 'params.schema', 'params.database',
                       'params.path', 
                       'creationTag.lastModifiedBy.login', 'creationTag.lastModifiedOn',
                       'versionTag.lastModifiedBy.login',  'versionTag.lastModifiedOn',
                       'shortDesc', 'description', 'params.metastoreDatabaseName',
                       'params.folderSmartId', 'tags', 'featureGroup',
                      ]

        for pk in self.client.list_project_keys():
            project_handle = self.client.get_project(pk)
            self.objects_list[pk] = project_handle.list_datasets(as_type='objects', include_shared=True)


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # key_mapping = set()
        
         # iterate through each object
        for pk, proj_datasets in self.objects_list.items():
            project_handle = self.client.get_project(pk)

            for r in proj_datasets:
               # try:
                dataset_handle = project_handle.get_dataset(r.id)
                if not dataset_handle.exists():
                    yield {'projectKey': pk,
                           'name':       r.id,
                           'dataset_exists': False
                          }
                    
                    
                dataset_settings_handle = dataset_handle.get_settings() # can throw exception if dataset does not exist
                raw_data = dataset_settings_handle.get_raw()

                # key_mapping.update(list_keys_recursive(raw_data)) # debugging, mapping out all the different keys depending on the type of dataset

                next_row = extract_nested_keys(raw_data, self.__keys)

                next_row['num_metrics_checks'] = len(raw_data.get('metricsChecks').get('checks', []))
                next_row['num_columns']        = len(raw_data.get('schema').get('columns', []))
                next_row['column_names']       = [col["name"] for col in raw_data.get("schema", {}).get("columns", []) if "name" in col]
                next_row['creationTag.lastModifiedOn'] = int_to_datetime(next_row.get('creationTag.lastModifiedOn', None))
                next_row['versionTag.lastModifiedOn']  = int_to_datetime(next_row.get('versionTag.lastModifiedOn',  None))
                next_row['dataset_exists'] = True

                #except Exception as e:
                    # com.dataiku.dip.server.controllers.NotFoundException
                    # Test failed: com.dataiku.dip.server.controllers.NotFoundException: dataset does not exist: HCP_TARGET_AGENTS.Sales_Marketing_queries
                 #   print(f"Exception with dataset {r.id} in project {pk}: {e}")
                 #   continue

                # return a single row
                yield next_row
                
        # print_sorted_strings(key_mapping)
            
####################################################################
# Same for all instances:
####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        return len(self.objects_list)

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
