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
        self.unique_id_key_name = 'name'
        self.keys   = [self.unique_id_key_name, 'type',  'tags', 'labels', 'projectKey'] 
        #'params.engineType', 'creationTag.lastModifiedOn', 'versionTag.lastModifiedOn', 
        self.projectkeys = self.client.list_project_keys()
        self.objects_list = {}
        
        for pk in self.projectkeys:
            project_handle = self.client.get_project(pk)
            # class 'dataikuapi.dss.recipe.DSSRecipeListItem
            # self.objects_list[pk] = project_handle.list_recipes(as_type='listitems')
            self.objects_list[pk] = project_handle.list_recipes(as_type='objects')
        

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for pk, proj_recipes in self.objects_list.items():
            project_handle = self.client.get_project(pk)

            for r in proj_recipes:
                recipe_id = r.id
                recipe_handle = project_handle.get_recipe(recipe_id)
                raw_data = recipe_handle.get_settings().get_recipe_raw_definition()
                
                pp(raw_data)
                next_row = flatten_dict(raw_data, include_keys=self.keys)
                
#                 next_row = {'projectKey': pk,
#                             'id': recipe_id,
#                            #'type': recipe['type'],
#                            #'name': recipe.name,
#                            # 'output_dataset': recipe.get('outputs',None).get('main',None).get('items',None).get('ref'),
#                             #'engineType': recipe['params']['engineType']
#                            }
                pp(next_row)
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
