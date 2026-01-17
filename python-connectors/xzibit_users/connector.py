"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    flatten_dict,
    get_dss_base_url,
    parse_user_datetime,
    int_to_datetime,
)


class ConnectorUsers(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, id):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, id)):
            return None
        return f"{self.__baseurl}/admin/security/users/edit/{id}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0
        unique_id_key_name = "login"
        keys = [
            unique_id_key_name,
            "displayName",
            "userProfile",
            "groups",
            "sourceType",
            "email",
            "creationDate",
            "enabled",
            "resultingUserProfile",
            "userProfile",
        ]
        # iterate through each object
        for item_info in self.__client.list_users():
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return

                next_row = flatten_dict(item_info, include_keys=keys)

                item_handle = self.__client.get_user(item_info[unique_id_key_name])
                # TODO: fix this date mess below
                next_row["last_successful_login"] = parse_user_datetime(
                    str(item_handle.get_activity().last_successful_login)
                )
                next_row["last_session_activity"] = parse_user_datetime(
                    str(item_handle.get_activity().last_session_activity)
                )
                next_row["url"] = self.get_url(next_row.get("login", None))

                # TODO: bug inside int_to_datetime
                next_row["created_timestamp"] = int_to_datetime(
                    next_row.get("creationDate", 0)
                )
                # pp(item_info)
            except Exception as e:
                # TODO: figure out why this is getting hit so much
                print(
                    f"[users-generate_rows] [UNEXPECTED EXCEPTION] {e} with user {next_row.get('login', None)}"
                )
            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date
        return {
            "columns": [
                {"name": "login", "type": "string", "meaning": "Text"},
                {"name": "displayName", "type": "string", "meaning": "Text"},
                {"name": "userProfile", "type": "string", "meaning": "Text"},
                {
                    "name": "groups",
                    "type": "string",
                    "meaning": "JSONArrayMeaning",
                },
                {"name": "sourceType", "type": "string", "meaning": "Text"},
                {  # intentionally not setting the meanting=Email b/c email format is not enforced for DSS users, and there are exceptions to this on Dev-Design
                    "name": "email",
                    "type": "string",
                    "meaning": "Text",
                },
                {"name": "enabled", "type": "boolean", "meaning": "Boolean"},
                {"name": "resultingUserProfile", "type": "string", "meaning": "Text"},
                {
                    "name": "created_timestamp",
                    "type": "string",
                    "meaning": "Text",
                },
                {"name": "last_successful_login", "type": "date", "meaning": "Date"},
                {"name": "last_session_activity", "type": "date", "meaning": "Date"},
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
