# This file is the actual code for the custom Python dataset xzibit_agenttooltypes

# import the base class for the custom dataset
from six.moves import xrange
from dataiku.connector import Connector

"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant.
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
        Connector.__init__(
            self, config, plugin_config
        )  # pass the parameters to the base class

        # perform some more initialization
        self.theparam1 = self.config.get("parameter1", "defaultValue")

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

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        for i in xrange(1, 10):
            yield {"first_col": str(i), "my_string": "Yes"}

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {
                    "meaning": "Boolean",
                    "name": "onlyLimitedVisibility",
                    "type": "boolean",
                },
                {"meaning": "Text", "name": "appId", "type": "string"},
                {"meaning": "Text", "name": "appVersion", "type": "string"},
                {"meaning": "Text", "name": "label", "type": "string"},
                {"meaning": "FreeText", "name": "shortDesc", "type": "string"},
                {"meaning": "JSONArrayMeaning", "name": "tags", "type": "string"},
                {"meaning": "Boolean", "name": "isAppImg", "type": "boolean"},
                {"meaning": "Text", "name": "origin", "type": "string"},
                {"meaning": "Text", "name": "originProjectKey", "type": "string"},
                {"meaning": "LongMeaning", "name": "instanceCount", "type": "int"},
                {"meaning": "Boolean", "name": "useAsRecipe", "type": "boolean"},
                {"meaning": "URL", "name": "url", "type": "string"},
            ]
        }

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
