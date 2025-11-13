####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *

####################################################################
# Unique imports for this Class
####################################################################
from xzibit.utils import get_path_size_megabytes

class ConnectorCodeEnvs(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        
        self.client = api_client()
        self.unique_id_key_name = 'envName'
        self.keys   = [self.unique_id_key_name, 'envLang', 'deploymentMode', 'pythonInterpreter', 'owner' ]
        self.objects_list = self.client.list_code_envs()

            
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.objects_list:
            next_row = flatten_dict(item_info, include_keys=self.keys)
            
            # custom things for this specific class:
            env_lang = next_row['envLang']
            env_name = next_row['envName']
            try:
                code_env_handle = self.client.get_code_env(env_lang, env_name)
                settings = code_env_handle.get_settings().get_raw()
                next_row['corePackagesSet'] = settings.get('desc',[]).get('corePackagesSet',[])
                next_row['path']            = settings.get('path', None)

                next_row['disk_size_megabytes'] = get_path_size_megabytes(next_row['path'])

                list_of_usages = code_env_handle.list_usages()

                if len(list_of_usages) == 0:
                    next_row['usages'] = []
                else:
                    next_row['usages'] = list(get_values_for_key(list_of_usages, 'projectKey')) 

            except Exception as e:
                print(f"Exception {e} with code_env_info:")
                pp(item_info)

            finally:
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
