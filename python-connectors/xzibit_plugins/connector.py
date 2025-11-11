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
#

class ConnectorPlugins(Connector):

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        
        self.client = api_client()
        self.unique_id_key_name = 'id'
        self.keys   = [self.unique_id_key_name, 'meta.label', 'version', 'meta.author', 'meta.tags', 'meta.description', 'isDev']
        self.objects_list = self.client.list_projects()

            
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        # iterate through each object
        for item_info in self.objects_list:
            next_row = flatten_dict(item_info, include_keys=self.keys)
            
            # custom things for this specific class:
            next_row = remove_prefix_from_keys(next_row, 'meta.')
            
            pp(next_row)

            plugin_handle = self.client.get_plugin(next_row['id'])

            list_of_usages = plugin_handle.list_usages().get_raw()['usages']

            if len(list_of_usages) == 0:
                next_row['project_usages'] = []
            else:
                next_row['project_usages'] = list(get_values_for_key(list_of_usages, 'projectKey')) 

            next_row['total_usages'] = len(list_of_usages)
            
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

    
    
    
    
    
    
    


# # This file is the actual code for the custom Python dataset xzibit_plugins

# from dataiku import api_client
# from dataiku.connector import Connector
# from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key, list_to_error_dict, pp

# """
# A custom Python dataset is a subclass of Connector.

# The parameters it expects and some flags to control its handling by DSS are
# specified in the connector.json file.

# Note: the name of the class itself is not relevant.
# """
# class MyConnector(Connector):

#     def __init__(self, config, plugin_config):
#         """
#         The configuration parameters set up by the user in the settings tab of the
#         dataset are passed as a json object 'config' to the constructor.
#         The static configuration parameters set up by the developer in the optional
#         file settings.json at the root of the plugin directory are passed as a json
#         object 'plugin_config' to the constructor
#         """
#         Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
#         self.client = api_client()
    

#     def get_read_schema(self):
#         """
#         Returns the schema that this connector generates when returning rows.

#         The returned schema may be None if the schema is not known in advance.
#         In that case, the dataset schema will be infered from the first rows.

#         If you do provide a schema here, all columns defined in the schema
#         will always be present in the output (with None value),
#         even if you don't provide a value in generate_rows

#         The schema must be a dict, with a single key: "columns", containing an array of
#         {'name':name, 'type' : type}.

#         Example:
#             return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

#         Supported types are: string, int, bigint, float, double, date, boolean
#         """
#         return None


#     def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
#                             partition_id=None, records_limit = -1):
#         """
#         The main reading method.
#         """
#         keys = ['meta.label', 'id', 'version', 'meta.author', 'meta.tags', 'meta.description', 'isDev']
#         for plugin_info in self.client.list_plugins():
#             try:
#                 next_row = flatten_dict(plugin_info, 
#                                    include_keys=keys)
#                 next_row = remove_prefix_from_keys(next_row, 'meta.')

#                 plugin_handle = self.client.get_plugin(next_row['id'])

#                 # raw =  plugin_handle.list_usages().get_raw()
#                 # pp(raw)

#                 list_of_usages = plugin_handle.list_usages().get_raw()['usages']

#                 if len(list_of_usages) == 0:
#                     next_row['project_usages'] = []
#                 else:
#                     next_row['project_usages'] = list(get_values_for_key(list_of_usages, 'projectKey')) 

#                 next_row['total_usages'] = len(list_of_usages)
#             except Exception as e:
#                 print(f"Exception {e} with plugin_info:")
#                 # pprint(plugin_info)
#                 next_row = list_to_error_dict(keys)
#             finally:
#                 yield next_row


#     def get_partitioning(self):
#         """
#         Return the partitioning schema that the connector defines.
#         """
#         raise NotImplementedError


#     def list_partitions(self, partitioning):
#         """Return the list of partitions for the partitioning scheme
#         passed as parameter"""
#         return []


#     def partition_exists(self, partitioning, partition_id):
#         """Return whether the partition passed as parameter exists

#         Implementation is only required if the corresponding flag is set to True
#         in the connector definition
#         """
#         raise NotImplementedError


#     def get_records_count(self, partitioning=None, partition_id=None):
#         """
#         Returns the count of records for the dataset (or a partition).

#         Implementation is only required if the corresponding flag is set to True
#         in the connector definition
#         """
#         return len(self.client.list_plugins())
