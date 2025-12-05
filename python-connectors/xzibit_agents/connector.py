####################################################################
# Unique imports for this Class
####################################################################
from datetime import datetime

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector

# from xzibit.utils import *
from xzibit.utils import get_dss_base_url


def parse_llm_id(llm_string: str):
    """
    Splits a string by ':' into exactly 3 variables.
    Returns None for missing fields.
    """
    if not llm_string:
        return None, None, None

    # Split the string
    parts = llm_string.split(":")

    # Pad the list with None to ensure it has at least 3 elements,
    # then slice to take exactly the first 3.
    # This handles cases with 1, 2, or 3+ segments gracefully.
    padded = (parts + [None] * 3)[:3]

    return padded[0], padded[1], padded[2]


class ConnectorProjects(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()

    def get_url(self, id, project_key, agent_version):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, id, project_key, agent_version)):
            return None
        return f"{self.__baseurl}/projects/{project_key}/savedmodels/{id}/agent/S-{project_key}-{id}-{agent_version}"

    # pylint: disable=W0613
    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0

        # iterate through each object
        for project_key in self.__client.list_project_keys():
            if records_limit > 0 and records_generated >= records_limit:
                break

            try:
                project = self.__client.get_project(project_key)

                # List all agents in the current project
                # Note: This returns a list of DSSAgentListItem objects
                agents = project.list_agents()

                for agent_item in agents:
                    try:
                        if records_limit > 0 and records_generated >= records_limit:
                            break

                        next_row = {
                            "projectKey": project_key,
                            "agent_name": agent_item.name,
                            "agent_id": agent_item.id,
                        }
                        # Get the full agent object and its settings
                        # We need the full object to access .get_settings()
                        agent = project.get_agent(agent_item.id)
                        settings = agent.get_settings()
                        # raw_settings = settings.get_raw()

                        # We typically want the LLM used by the *Active* version of the agent
                        next_row["active_agent_version"] = settings.active_version
                        active_version_id = settings.active_version

                        next_row["llm_model_id"] = "Unknown"
                        llm_model_id = "Unknown"
                        # is_active_version = active_version_id == agent_item.get(
                        #     "activeVersion", "Unknown"
                        # )
                        next_row["creation_user"] = "Unknown"

                        creation_user = "Unknown"

                        if active_version_id:
                            # Retrieve settings for the active version
                            version_settings = settings.get_version_settings(
                                active_version_id
                            )

                            # 1. Try standard Visual Agent property
                            try:
                                # the following line occasionally throws Exception
                                llm_model_id = version_settings.get("llm_id", None)

                            except AttributeError:
                                # 2. Fallback: Check raw settings (common for Code Agents)
                                ver_raw = version_settings.get_raw()
                                llm_model_id = ver_raw.get("llmId", None)

                                # Sometimes stored under 'generation' block for complex setups
                                if not llm_model_id and "generation" in ver_raw:
                                    llm_model_id = ver_raw["generation"].get(
                                        "llmId", None
                                    )

                            next_row["llm_model_id"] = llm_model_id

                            creation_user = (
                                version_settings.get_raw()
                                .get("creationTag", {})
                                .get("lastModifiedBy", {})
                                .get("login", None)
                            )
                            next_row["creation_user"] = creation_user

                            last_modified_on = datetime.fromtimestamp(
                                version_settings.get_raw()
                                .get("versionTag", {})
                                .get("lastModifiedOn", 0)
                                // 1000
                            )
                            next_row["last_modified_on"] = last_modified_on

                            last_modified_user = (
                                version_settings.get_raw()
                                .get("versionTag", {})
                                .get("lastModifiedBy", {})
                                .get("login", None)
                            )
                            next_row["last_modified_user"] = last_modified_user
                        # pp(raw_settings)

                        agent_version = agent_item.get("activeVersion", None)
                        llm_vendor, llm_connection_name, llm_model = parse_llm_id(
                            llm_model_id
                        )

                        next_row = {
                            "projectKey": project_key,
                            "agent_name": agent_item.name,
                            "agent_id": agent_item.id,
                            "creator_user": creation_user,
                            "last_modified_user": last_modified_user,
                            "active_agent_version": active_version_id,
                            # "LLM Model ID": llm_model_id,
                            "llm_vendor": llm_vendor,
                            "llm_connection_name": llm_connection_name,
                            "llm_model": llm_model,
                            "agent_type": agent_item.get("type", None),
                            "agent_version": agent_version,
                            "tags": agent_item.get("tags", None),
                            "last_modified_timestamp": last_modified_on,
                            # "Agent is active version": is_active_version,
                            "url": self.get_url(
                                agent_item.id, project_key, agent_version
                            ),
                        }
                    except (AttributeError, KeyError, TypeError, ValueError) as e_agent:
                        print(
                            f"[agents-generate_rows] [EXPECTED EXCEPTION] {e_agent} - Project Key: {project_key}, Agent Name: {agent_item.name}"
                        )
                    finally:
                        records_generated += 1
                        yield next_row

            except Exception as e_proj:
                # Pass on projects where we lack permissions or feature is disabled
                print(
                    f"[agents-generate_rows] [UNEXPECTED EXCEPTION] {e_proj} - Project Key: {project_key}"
                )

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, DatetimeNoTz, Date, FreeText, LongMeaning
        return {
            "columns": [
                {"meaning": "Text", "name": "agent_name", "type": "string"},
                {"meaning": "Text", "name": "agent_id", "type": "string"},
                {"meaning": "Text", "name": "projectKey", "type": "string"},
                {"meaning": "Text", "name": "llm_vendor", "type": "string"},
                {"meaning": "Text", "name": "llm_connection_name", "type": "string"},
                {"meaning": "Text", "name": "llm_model", "type": "string"},
                {"meaning": "Text", "name": "agent_type", "type": "string"},
                {"meaning": "Text", "name": "agent_version", "type": "string"},
                {"meaning": "Text", "name": "creator_user", "type": "string"},
                {"meaning": "Text", "name": "last_modified_user", "type": "string"},
                {"meaning": "JSONArrayMeaning", "name": "tags", "type": "string"},
                {
                    "meaning": "DatetimeNoTz",
                    "name": "last_modified_timestamp",
                    "type": "datetimenotz",
                },
                {"meaning": "Text", "name": "active_agent_version", "type": "string"},
                {"meaning": "URL", "name": "url", "type": "string"},
            ],
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
