####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key
from xzibit.utils import pp


####################################################################
# Unique imports for this Class
####################################################################
# none.

class ConnectorRecipes(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        
        self.client = api_client()
        self.unique_id_key_name = 'id'
        self.keys   = [self.unique_id_key_name, 'name']
        self.projectkeys = self.client.list_project_keys()
        self.objects_list = {}
        
        for pk in self.projectkeys:
            project_handle = self.client.get_project(pk)
            self.objects_list[pk] = project_handle.list_recipes(as_type='listitems')
        

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for pk, proj_recipes in self.objects_list.items():
            # next_row = flatten_dict(item_info, include_keys=self.keys)
            for recipe in project_recipes:
                pp(recipe)
#                next_row = {}
                next_row = {'projectkey': pk, 'name', recipe['']}
            
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
