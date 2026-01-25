"""TBD"""

####################################################################
# Same imports for all dataset Classes
####################################################################
from dataiku import api_client
from dataiku.connector import Connector
from xzibit.utils import (
    get_dss_base_url,
    flatten_dict,
    get_values_for_key,
    get_path_size_megabytes,
    pp,
)
from xzibit.deprecations import load_local_csv_as_dataframe, lookup_python_support


class ConnectorCodeEnvs(Connector):
    """TBD"""

    ####################################################################
    # Code that has to be customized for this specific class
    ####################################################################
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__df_dss_python = load_local_csv_as_dataframe(
            "DSS_version_python_support.csv"
        )
        # print("Loaded DSS_version_python_support.csv:")

        # Calculating disk space usage on my personal FM instance:
        #    * took 8 seconds
        #    * 94.0 GB of total code environment disk space
        #    * for 48 unique code environments
        #    * ... average code env size was 2005.3 GB
        #    * ... average time to calculate each code env size: 0.16667 sec on second run. Unsure of first run

        self.__compute_codeenv_disk_space_usage = self.config.get(
            "compute_codeenv_disk_space_usage", False
        )
        self.__compute_codeenv_usages = self.config.get("compute_codeenv_usages", False)

    def get_url(self, env_name, env_lang="python"):
        """Create a URL to the DSS object in question in this specific DSS instance.
        Return None if any of the inputs are None."""
        # at least one is None, return None
        if any(v is None for v in (self.__baseurl, env_lang, env_name)):
            return None
        # trailing slash is MANDATORY for Code Envs
        return f"{self.__baseurl}/admin/code-envs/design/{env_lang.lower()}/{env_name}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
        """TBD"""
        records_generated = 0

        for code_env_handle in self.__client.list_code_envs(as_objects=True):
            try:
                if records_limit > 0 and records_generated >= records_limit:
                    return
                settings = code_env_handle.get_settings()

                code_env_name = settings.env_name
                code_env_lang = settings.env_lang

                next_row = {
                    "code_env_name": code_env_name,
                    "code_env_lang": code_env_lang,
                }
                next_row["url"] = self.get_url(code_env_name, code_env_lang)

                settings_raw = settings.get_raw()
                # pp(settings_raw)
                next_row["deployment_mode"] = settings_raw.get("deploymentMode", None)
                next_row["python_interpreter"] = settings_raw.get("desc", {}).get(
                    "pythonInterpreter", None
                )
                next_row["owner"] = settings_raw.get("owner", None)
                next_row["core_packages_set"] = settings_raw.get("desc", {}).get(
                    "corePackagesSet", None
                )
                next_row["path"] = settings_raw.get("path", None)

                if next_row["code_env_lang"] == "R":
                    next_row["python_version_support_status"] = "N/A"
                else:

                    if next_row["python_interpreter"] is not None:
                        # next_row["python_interpreter"] takes the form of PYTHON39 or PYTHON310
                        python_version_formatted = next_row[
                            "python_interpreter"
                        ].replace("PYTHON", "")

                        # add a . as the second character in python_version_formatted
                        python_version_formatted = (
                            python_version_formatted[0]
                            + "."
                            + python_version_formatted[1:]
                        )

                        next_row["python_version_support_status"] = (
                            lookup_python_support(
                                "14", python_version_formatted, self.__df_dss_python
                            )
                        )
                    else:
                        next_row["python_version_support_status"] = "Unknown"

                    # next_row["python_version_support_status"] = lookup_python_support(
                    #     "14", "3.10", self.__df_dss_python
                    # )

                if self.__compute_codeenv_disk_space_usage:
                    # get_path_size_megabytes returns 0 if path does not exist
                    next_row["size_in_MB"] = get_path_size_megabytes(next_row["path"])
                else:
                    next_row["size_in_MB"] = None

                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # adding list_usages for code environments on DevDesign (600 code env at an
                # average of 30 sec per code env to list all its usages across 2,362 projects), increases
                # the dataset's built time from 2 min 30 sec to 5 hours!!!
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

                if self.__compute_codeenv_usages:

                    next_row["projectKeys_where_code_env_used"] = []
                    next_row["total_instances_of_code_env"] = -1

                    # print(f"starting code env list usages for {code_env_name}")
                    # next line throws exception on DevDesign:
                    #  jakarta.servlet.ServletException: Handler dispatch failed: java.lang.Error: Unknown tool type: Custom_agent_tool_jira-tools_jira-create-issue-tool, caused by: Error: Unknown tool type: Custom_agent_tool_jira-tools_jira-create-issue-tool
                    # list_usages() does not take any parameters
                    usages = code_env_handle.list_usages()
                    # print(f"Finished code env list usages for {code_env_name}")
                    num_usages = len(usages)
                    if len(usages) == 0:
                        pk_usages = None
                    else:
                        pk_usages = list(get_values_for_key(usages, "projectKey"))
                    next_row["projectKeys_where_code_env_used"] = pk_usages
                    next_row["total_instances_of_code_env"] = num_usages

                else:
                    next_row["projectKeys_where_code_env_used"] = None
                    next_row["total_instances_of_code_env"] = None

            except Exception as e:
                # this is occuring on DevDesign
                print(
                    f"codeenvs - generate_rows EXCEPTION: CodeEnv: {code_env_name} Error message: {e}"
                )

            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        """TBD"""
        # Data types: https://developer.dataiku.com/latest/api-reference/python/datasets.html#dataiku.core.dataset.Schema
        # Meanings: Text, JSONArrayMeaning, Email, Boolean, Date, FreeText, LongMeaning, DoubleMeaning
        return None
        # return {
        #     "columns": [
        #         {"meaning": "Text", "name": "code_env_name", "type": "string"},
        #         {"meaning": "Text", "name": "code_env_lang", "type": "string"},
        #         {"meaning": "Text", "name": "deployment_mode", "type": "string"},
        #         {"meaning": "Text", "name": "owner", "type": "string"},
        #         {"meaning": "Text", "name": "python_interpreter", "type": "string"},
        #         {"meaning": "Text", "name": "core_packages_set", "type": "string"},
        #         {"meaning": "Text", "name": "path", "type": "string"},
        #         {"meaning": "DoubleMeaning", "name": "size_in_MB", "type": "double"},
        #         {
        #             "name": "projectKeys_where_code_env_used",
        #             "meaning": "JSONArrayMeaning",
        #             "type": "string",
        #         },
        #         {
        #             "name": "total_instances_of_code_env",
        #             "meaning": "LongMeaning",
        #             "type": "bigint",
        #         },
        #         {"meaning": "URL", "name": "url", "type": "string"},
        #     ]
        # }

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
