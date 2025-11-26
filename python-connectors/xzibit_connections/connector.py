####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import *


class ConnectorConnections(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__keys = [
            "name",
            "type",
            "usableBy",
            "allowWrite",
            "credentialsMode",
            "params.credentialsMode",
            "creationTag.lastModifiedBy.login",
            "creationTag.lastModifiedBy.lastModifiedOn",
            "params.authType",
            "params.db",
            "params.defaultSchema",
            "params.role",
            "params.warehouse",
            "params.scope",
        ]
        # self.__objects_list = self.__client.list_connections(as_type='listitems')

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        # iterate through each object
        for item_info in self.__client.list_connections(as_type="listitems"):
            # pp(item_info)
            next_row = flatten_dict(item_info, include_keys=self.__keys)
            yield next_row

    ####################################################################
    # Same for all instances:
    ####################################################################
    def get_records_count(self, partitioning=None, partition_id=None):
        """TBD"""
        return len(self.__client.list_connections())

    ####################################################################
    # Intentionally not implemented, not needed for this type
    ####################################################################
    def get_partitioning(self):
        """TBD"""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """TBD"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """TBD"""
        raise NotImplementedError

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {'columns': [{'meaning': 'Text', 'name': 'params.db', 'type': 'string'},
             {'meaning': 'Text', 'name': 'name', 'type': 'string'},
             {'meaning': 'Text', 'name': 'type', 'type': 'string'},
             {'meaning': 'Text',
              'name': 'creationTag.lastModifiedBy.login',
              'type': 'string'},
             {'meaning': 'Boolean', 'name': 'allowWrite', 'type': 'boolean'},
             {'meaning': 'Text', 'name': 'credentialsMode', 'type': 'string'},
             {'meaning': 'Text', 'name': 'usableBy', 'type': 'string'},
             {'meaning': 'Text',
              'name': 'params.credentialsMode',
              'type': 'string'},
             {'meaning': 'Text', 'name': 'params.authType', 'type': 'string'},
             {'meaning': 'Text', 'name': 'params.scopes', 'type': 'string'},
             {'meaning': 'Text', 'name': 'params.scope', 'type': 'string'},
             {'meaning': 'Text', 'name': 'params.warehouse', 'type': 'string'},
             {'meaning': 'Text', 'name': 'params.role', 'type': 'string'},
             {'meaning': 'Text',
              'name': 'params.defaultSchema',
              'type': 'string'}]}
