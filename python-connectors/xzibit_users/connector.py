"""Connector that provides a dataset of all Users on the DSS instance."""

from dataiku import api_client
from dataiku.connector import Connector

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import (
    flatten_dict,
    get_dss_base_url,
    parse_user_datetime,
    int_to_datetime,
)


class ConnectorUsers(XzibitBaseConnector, Connector):
    """Connector that provides a dataset of all Users on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, login):
        """Returns the DSS UI URL for the user's admin page, or None if inputs are missing."""
        if any(v is None for v in (self.__baseurl, login)):
            return None
        return f"{self.__baseurl}/admin/security/users/edit/{login}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
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
        for item_info in self.__client.list_users():
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return

                next_row = flatten_dict(item_info, include_keys=keys)
                item_handle = self.__client.get_user(item_info[unique_id_key_name])
                next_row["last_successful_login"] = parse_user_datetime(
                    str(item_handle.get_activity().last_successful_login)
                )
                next_row["last_session_activity"] = parse_user_datetime(
                    str(item_handle.get_activity().last_session_activity)
                )
                next_row["url"] = self.get_url(next_row.get("login", None))

                next_row["created_timestamp"] = int_to_datetime(
                    next_row.get("creationDate", 0)
                )
            except Exception as e:
                print(
                    f"[users-generate_rows] [UNEXPECTED EXCEPTION] {e} with user {next_row.get('login', None)}"
                )
            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
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
                {  # email format is not enforced for DSS users, so meaning=Text not Email
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
