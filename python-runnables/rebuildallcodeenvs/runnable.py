import json
import sys
import shutil
import glob
import os
from concurrent.futures import ThreadPoolExecutor

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
        # standard members
        self.__project_key   = project_key
        self.__config        = config
        self.__plugin_config = plugin_config

        # members specific to this class
        self.__client            = dataiku.api_client()
        self.__force_rebuild_env = False # will be configurable in future release
        self.__num_threads       = 1     # will be configurable in future release
        self.__failed_builds     = set()
        self.__successful_builds = set()
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None
    

    def _process_code_env(self, code_env_info):
        try:
            
            envName = code_env_info['envName']
            code_env = self.__client.get_code_env(code_env_info['envLang'], envName)

            # print(f'Starting rebuilding {envName} ...')
            # env_path = os.path.join('/data/dataiku/dss_data/code-envs/python', envName)

            # rebuild it
            res = code_env.update_packages(force_rebuild_env=self.__force_rebuild_env)

            if res['messages']['success']:
                print(f'Success: {envName}')
                successful_builds.add(envName)
            else:
                print(f"FAILED: {envName}")
                failed_builds.add(envName)

        except Exception as e:
            try:
                if not force_rebuild_env:
                    # print(f'Failed to build {envName} without force rebuild, trying again with force rebuild...')
                    res = code_env.update_packages(force_rebuild_env=True)
                    print(f'Success: {envName} when Force rebuild')
                    pass
            except Exception as e:
                print(f"FAILED: {envName}, even with force rebuild")
                failed_builds.add(envName) # potential bug where this doesn't happen, should use a finally clause
                pass


    
    
    def _rebuild_all_code_envs(self):
        """x"""
        



    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        # raise Exception("unimplemented")
        self._rebuild_all_code_envs()
        return None