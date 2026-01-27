"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    get_dss_base_url,
    flatten_dict,
    remove_prefix_from_keys,
    list_to_error_dict,
    pp,
    jd,
    get_values_for_key,
)

from xzibit.deprecations import DEPRECATED_PLUGIN_IDS


class ConnectorPlugins(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
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

        for pk in self.__client.list_project_keys():
            if records_limit > 0 and records_generated >= records_limit:
                return

            project_handle = self.__client.get_project(pk)
            for dataset_handle in project_handle.list_datasets():
                if records_limit > 0 and records_generated >= records_limit:
                    return

                try:

                    next_row = {
                        "object_type": "dataset",
                        "projectKey": pk,
                        "object_id": dataset_handle.id,
                    }

                    # dataset_info = dataset_handle.get_definition()

                except Exception:
                    next_row = {
                        "object_type": "dataset",
                        "projectKey": pk,
                        "object_id": "EXCEPTION",
                    }

                finally:
                    records_generated += 1
                    yield next_row

    def get_read_schema(self):
        """TBD"""
        return None

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
