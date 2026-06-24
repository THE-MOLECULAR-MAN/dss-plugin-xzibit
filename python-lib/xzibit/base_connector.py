"""Shared mixin for all xzibit plugin connectors.

None of the xzibit datasets support partitioning, so every connector would
otherwise repeat the same four boilerplate methods identically.  Mixing in
XzibitBaseConnector eliminates that duplication.

IMPORTANT — do NOT make this class inherit from dataiku.connector.Connector.
DSS discovers the active connector by scanning the module namespace for
subclasses of Connector.  If XzibitBaseConnector were a Connector subclass it
would appear in the scan alongside the real ConnectorXxx class and DSS would
raise "Multiple classes inheriting Connector defined".  Keeping this as a
plain mixin means DSS finds exactly one Connector subclass per module.

Usage in each connector module:
    from dataiku.connector import Connector
    from xzibit.base_connector import XzibitBaseConnector

    class ConnectorFoo(XzibitBaseConnector, Connector):
        ...
"""


class XzibitBaseConnector:
    """Plain mixin providing no-op implementations of the DSS partitioning
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
