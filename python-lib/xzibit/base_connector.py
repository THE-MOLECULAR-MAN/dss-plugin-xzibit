"""Shared base class for all xzibit plugin connectors.

None of the xzibit datasets support partitioning, so every connector would
otherwise repeat the same four boilerplate methods identically.  Inheriting
from XzibitBaseConnector eliminates that duplication while keeping each
connector's generate_rows() and get_read_schema() fully independent.
"""

from dataiku.connector import Connector


class XzibitBaseConnector(Connector):
    """Extends Connector with no-op implementations of the DSS partitioning
    interface.  Subclasses only need to implement generate_rows() and
    get_read_schema()."""

    def get_records_count(self, partitioning=None, partition_id=None):
        return None

    def get_partitioning(self):
        raise NotImplementedError

    def list_partitions(self, partitioning):
        return []

    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError
