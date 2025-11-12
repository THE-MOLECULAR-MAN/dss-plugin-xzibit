####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key, extract_nested_keys
from xzibit.utils import pp, jd


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
        self.__keys = ['name', 'type', 'tags', 'description', 'featureGroup', 'managed', 'formatType', 'projectKey', 'params.connection', 'params.mode', 'params.table', 'shortDesc', 'versionTag.lastModifiedBy.login', 'versionTag.lastModifiedBy.lastModifiedOn', 'creationTag.lastModifiedOn', 'creationTag.lastModifiedBy.login', 'params.path']

        for pk in self.client.list_project_keys():
            project_handle = self.client.get_project(pk)
            self.objects_list[pk] = project_handle.list_datasets(as_type='objects', include_shared=True)


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
         # iterate through each object
        for pk, proj_datasets in self.objects_list.items():
            project_handle = self.client.get_project(pk)

            for r in proj_datasets:
                try:
                    dataset_handle = project_handle.get_dataset(r.id)
                    dataset_settings_handle = dataset_handle.get_settings()
                    raw_data = dataset_settings_handle.get_raw()

                    # len(metricsChecks.checks)
                    # len(schema.columns)
                    next_row = extract_nested_keys(raw_data, self.__keys)
                    
                    num_metrics_checks = len(raw_data.get('metricsChecks').get('checks', []))
                    num_columns        = len(raw_data.get('schema').get('columns', []))
                    
                    next_row['num_metrics_checks'] = num_metrics_checks
                    next_row['num_columns']        = num_columns
                    
                    # 'Snowflake'
                    if next_row['type'] in ['PostgreSQL']:
                        pp(raw_data)

#                         #'data_steward': raw_data.get('data_steward', None),

                    
                except Exception as e:
                    # com.dataiku.dip.server.controllers.NotFoundException
                    print(f"Exception with dataset {r.id} in project {pk}: {e}")
                    continue

                # return a single row
                yield next_row
            
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
