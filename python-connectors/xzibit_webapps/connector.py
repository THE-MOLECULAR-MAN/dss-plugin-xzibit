"""Connector that provides a dataset of all Web Apps across every project."""

import re
from datetime import datetime

from dataiku import api_client

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import get_dss_base_url


def make_url_friendly(text):
    """Converts a string to a Dataiku URL-friendly format:
    1. Converts to lower case
    2. Removes all characters except alphanumeric (letters/numbers) and spaces
    3. Replaces spaces (and runs of spaces) with a single hyphen
    """
    if not isinstance(text, str):
        return str(text)

    text = text.lower()
    # Keep only alphanumeric characters and spaces
    text = re.sub(r"[^a-z0-9\s]", "", text)
    # Replace one or more whitespace characters with a single hyphen
    text = re.sub(r"\s+", "-", text)

    return text


class ConnectorWebApps(XzibitBaseConnector):
    """Connector that provides a dataset of all Web Apps on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, project_key, webapp_id, web_app_name):
        """Returns the DSS UI URL for the web app, or None if inputs are missing."""
        if any(
            v is None for v in (self.__baseurl, webapp_id, project_key, web_app_name)
        ):
            return None
        safe_name = make_url_friendly(web_app_name)
        return f"{self.__baseurl}/projects/{project_key}/webapps/{webapp_id}_{safe_name}/edit"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        records_generated = 0
        for project_key in self.__client.list_project_keys():
            try:
                project = self.__client.get_project(project_key)
                if records_limit > 0 and records_generated >= records_limit:
                    return
                for webapp in project.list_webapps():
                    try:
                        if records_limit > 0 and records_generated >= records_limit:
                            return

                        next_row = {
                            "projectKey": project_key,
                            "webapp_name": webapp.get("name", None),
                            "webapp_id": webapp.get("id", None),
                            "webapp_type": webapp.get("type", None),
                            "created_by_user": webapp.get("createdBy", {}).get(
                                "login", None
                            ),
                            "backend_running": webapp.get("backendRunning", None),
                            "url": self.get_url(
                                project_key,
                                webapp.get("id", ""),
                                webapp.get("name", ""),
                            ),
                            "created_timestamp": datetime.fromtimestamp(
                                webapp.get("createdOn", 0) // 1000
                            ),
                            "last_modified_user": webapp.get("lastModifiedBy", {}).get(
                                "login", None
                            ),
                            "last_modified_timestamp": datetime.fromtimestamp(
                                webapp.get("lastModifiedOn", 0) // 1000
                            ),
                            "tags": webapp.get("tags", None),
                            "is_code_webapp": webapp.get("type")
                            in ["SHINY", "STANDARD", "BOKEH", "DASH"],
                        }
                        records_generated += 1
                        yield next_row
                    except Exception as e:
                        print(
                            f"[webapps-generate_rows] [UNEXPECTED WEBAPP EXCEPTION] {e} with webapp {next_row.get('webapp_name', None)}"
                        )

            except Exception as e:
                print(
                    f"[webapps-generate_rows] [UNEXPECTED PROJECT EXCEPTION] {e} with project {project_key}"
                )

    def get_read_schema(self):
        return {
            "columns": [
                {"name": "webapp_id", "type": "string", "meaning": "Text"},
                {"name": "webapp_name", "type": "string", "meaning": "Text"},
                {"name": "projectKey", "type": "string", "meaning": "Text"},
                {"name": "webapp_type", "type": "string", "meaning": "Text"},
                {"name": "backend_running", "type": "boolean", "meaning": "Boolean"},
                {"name": "created_by_user", "type": "string", "meaning": "Text"},
                {"name": "last_modified_user", "type": "string", "meaning": "Text"},
                {
                    "name": "created_timestamp",
                    "type": "string",
                    "meaning": "Text",
                },
                {
                    "name": "last_modified_timestamp",
                    "type": "string",
                    "meaning": "Text",
                },
                {"name": "tags", "type": "string", "meaning": "JSONArrayMeaning"},
                {"name": "is_code_webapp", "type": "boolean", "meaning": "Boolean"},
                {"name": "url", "type": "string", "meaning": "URL"},
            ]
        }
