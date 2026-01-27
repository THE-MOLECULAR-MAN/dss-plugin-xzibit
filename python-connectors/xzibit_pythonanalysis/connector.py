"""TBD"""

from dataiku import api_client
from dataiku.connector import Connector


class ConnectorPythonAnalysis(Connector):
    """A Dataiku DSS v12 connector to provide a DSS Dataset listing
    all Python Code Recipes, and information about them."""

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
        for project_key in self.__client.list_projects():
            if records_limit > 0 and records_generated >= records_limit:
                return
            try:
                project_handle = self.__client.get_project(project_key)
                project_recipes = project_handle.list_recipes()
                
                
            except Exception as e:
                print(f"Error accessing project {project_key}: {e}")
                continue

            records_generated += 1
            yield next_row

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################

    def get_read_schema(self):
        """TBD"""
        return None

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
