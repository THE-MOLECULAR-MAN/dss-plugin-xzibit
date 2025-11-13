
import json
import sys

# from json import dumps
import traceback

# import pandas as pd
import dataiku
from dataiku.runnables import Runnable
from dataikuapi.utils import DataikuException
import dataikuapi


class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.__project_key   = project_key
        self.__config        = config
        self.__plugin_config = plugin_config
        self.__client        = dataiku.api_client()
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None


    def _sync_with_github(self):
        """x"""
        
        
        projects = self.__client.list_projects()
        successful = set()
        not_connected = set()
        errored = set()

        projects_analysis = []

        for iter_project_key in self.__client.list_project_keys():
            try:
                proj = self.__client.get_project(iter_project_key)
                project_git = proj.get_project_git()
                r = project_git.get_remote()
                status = project_git.get_status()

                has_github_repo = len(status.get('remotes',[])) > 0
                projects_analysis.append({"key":iter_project_key, "has_github_repo": has_github_repo, "status": str(status), "remote": r})

                if has_github_repo:
                    res_push = project_git.push()
                    res_pull = project_git.pull()

                    if (not res_push.get('success',False)) or (not res_pull.get('success',False)):
        #             if not res_push.get('success',False):
                        print(f"[ERROR] pushing or pulling {iter_project_key}")
                        errored.add(iter_project_key)
                        continue
                    successful.add(iter_project_key)
                else:
                    print(f"{iter_project_key} is not connected to GitHub")
                    not_connected.add(iter_project_key)
            except Exception as e:
                print(f"[EXCEPTION] {iter_project_key}: {e}")
                errored.add(iter_project_key)
                continue




    def run(self, progress_callback):
        """
        """
        self._sync_with_github()
        return None