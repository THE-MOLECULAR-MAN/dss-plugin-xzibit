"""Connector that provides a dataset of all Code Environments on the DSS instance."""

from dataiku import api_client
from dataiku.connector import Connector

from xzibit.base_connector import XzibitBaseConnector
from xzibit.utils import (
    get_dss_base_url,
    flatten_dict,
    get_values_for_key,
    get_path_size_megabytes,
    pp,
)
from xzibit.deprecations import load_local_csv_as_dataframe, lookup_python_support


class ConnectorCodeEnvs(XzibitBaseConnector, Connector):
    """Connector that provides a dataset of all Code Environments on the DSS instance."""

    def __init__(self, config, plugin_config):
        super().__init__(config, plugin_config)
        self.__client = api_client()
        self.__baseurl = get_dss_base_url()
        self.__df_dss_python = load_local_csv_as_dataframe(
            "DSS_version_python_support.csv"
        )

        # Calculating disk space usage on my personal FM instance:
        #    * took 8 seconds
        #    * 94.0 GB of total code environment disk space
        #    * for 48 unique code environments
        #    * ... average code env size was 2005.3 GB
        #    * ... average time to calculate each code env size: 0.16667 sec on second run. Unsure of first run

        self.__compute_codeenv_disk_space_usage = self.config.get(
            "compute_codeenv_disk_space_usage", False
        )

    def get_url(self, env_name, env_lang="python"):
        """Returns the DSS UI URL for the code environment, or None if inputs are missing.

        Code environment URLs require a trailing slash.
        """
        if any(v is None for v in (self.__baseurl, env_lang, env_name)):
            return None
        return f"{self.__baseurl}/admin/code-envs/design/{env_lang.lower()}/{env_name}/"

    def generate_rows(
        self,
        dataset_schema=None,
        dataset_partitioning=None,
        partition_id=None,
        records_limit=-1,
    ):
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

                if self.__compute_codeenv_disk_space_usage:
                    # get_path_size_megabytes returns 0 if path does not exist
                    next_row["size_in_MB"] = get_path_size_megabytes(next_row["path"])
                else:
                    next_row["size_in_MB"] = "DISABLED"

            except Exception as e:
                print(
                    f"codeenvs - generate_rows EXCEPTION: CodeEnv: {code_env_name} Error message: {e}"
                )

            finally:
                records_generated += 1
                yield next_row

    def get_read_schema(self):
        return None
