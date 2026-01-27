"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector


####################################################################
# Unique imports for this Class
####################################################################
from datetime import datetime


class ConnectorPythonAnalysis(Connector):
    """A Dataiku DSS v12 connector to provide a DSS Dataset listing all Python Code Recipes, and information about them."""

    ####################################################################
    # Code that has to be customized for this specific class
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0

        # iterate through each project
        for item_info in self.__client.list_projects():
            if records_limit > 0 and records_generated >= records_limit:
                return


            records_generated += 1
            yield next_row

    def get_read_schema(self):
        """TBD"""
        return {
            "columns": [
                {"name": "projectKey", "type": "string", "meaning": "Text"},
                {"name": "name", "type": "string", "meaning": "Text"},
                {"name": "shortDesc", "type": "string", "meaning": "FreeText"},
                {"name": "description", "type": "string", "meaning": "FreeText"},
                {"name": "ownerLogin", "type": "string", "meaning": "Text"},
                {"name": "projectStatus", "type": "string", "meaning": "Text"},
                {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
                {
                    "name": "last_modified_timestamp",
                    "type": "string",
                    "meaning": "Text",
                },
                {"name": "tutorialProject", "type": "boolean", "meaning": "Boolean"},
                {
                    "name": "contributors",
                    "type": "string",
                    "meaning": "JSONArrayMeaning",
                },
                {"name": "url", "type": "string", "meaning": "URL"},
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
