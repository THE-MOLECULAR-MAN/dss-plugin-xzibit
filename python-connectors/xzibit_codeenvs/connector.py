# This file is the actual code for the custom Python dataset dss-internals_code-envs

# import the base class for the custom dataset
from dataiku.connector import Connector
from dataiku import api_client
from xzibit.utils import get_values_from_list_of_dicts, flatten_dict, remove_prefix_from_keys, get_values_for_key, pp

"""
A custom Python dataset is a subclass of Connector.
"""
class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.client = api_client()

    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        If you do provide a schema here, all columns defined in the schema
        will always be present in the output (with None value),
        even if you don't provide a value in generate_rows

        The schema must be a dict, with a single key: "columns", containing an array of
        {'name':name, 'type' : type}.

        Example:
            return {"columns" : [ {"name": "col1", "type" : "string"}, {"name" :"col2", "type" : "float"}]}

        Supported types are: string, int, bigint, float, double, date, boolean
        """

        # In this example, we don't specify a schema here, so DSS will infer the schema
        # from the columns actually returned by the generate_rows method
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        for code_env_info in self.client.list_code_envs():
            # pp(code_env_info)
            next_code_env = flatten_dict(code_env_info, 
                               include_keys=['envName', 'envLang', 'deploymentMode', 'pythonInterpreter', 'owner', 'isUptodate'])
            env_lang = next_code_env['envLang']
            env_name = next_code_env['envName']
            
            code_env_handle = self.client.get_code_env(env_lang, env_name)
            settings = code_env_handle.get_settings().get_raw()
            next_code_env['corePackagesSet'] = settings.get('desc',[]).get('corePackagesSet',[])
            next_code_env['path'] = settings.get('path',None)
            
            pp(settings)
            
            list_of_usages = code_env_handle.list_usages()

            if len(list_of_usages) == 0:
                next_code_env['usages'] = []
            else:
                next_code_env['usages'] = list(get_values_for_key(list_of_usages, 'projectKey')) 

            yield next_code_env

    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise NotImplementedError


    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []


    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise NotImplementedError


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        return len(self.client.list_code_envs())
