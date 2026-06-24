"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    get_dss_base_url,
    pp,
    jd,
)

from xzibit.deprecations import DEPRECATED_PLUGIN_IDS


class ConnectorPluginUsages(Connector):
    """Connector that provides a dataset of all plugin usage instances across projects."""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        # self.__baseurl = get_dss_base_url()

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0

        # list_plugins does not offer any parameters
        # list_plugins returns a list of dict. Each dict contains at least a ‘id’ field
        for item_info in self.__client.list_plugins():
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return

                plugin_id = item_info.get("id", "NO_PLUGIN_ID")
                plugin_handle = self.__client.get_plugin(plugin_id)
                # settings_raw = plugin_handle.get_settings().get_raw()
                list_of_usages = plugin_handle.list_usages()

                for usage in list_of_usages.usages:
                    try:
                        if records_limit > 0 and records_generated >= records_limit:
                            return

                        next_row = {"plugin_id": plugin_id}
                        next_row["plugin_is_deprecated"] = (
                            plugin_id in DEPRECATED_PLUGIN_IDS
                        )
                        next_row["element_kind"] = usage.element_kind
                        next_row["element_type"] = usage.element_type
                        next_row["object_id"] = usage.object_id
                        next_row["object_type"] = usage.object_type
                        next_row["project_key"] = usage.project_key  # may not have one

                    except Exception as e:
                        print(
                            f"[plugin_usages-generate_rows] ! [UNEXPECTED EXCEPTION] {e} with plugin {plugin_id}"
                        )
                    finally:
                        records_generated += 1
                        yield next_row
            except Exception as e:
                print(
                    f"[plugin_usages-generate_rows] [UNEXPECTED EXCEPTION] {e} with plugin {plugin_id}"
                )

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
