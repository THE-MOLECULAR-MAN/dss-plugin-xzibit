# This file is the actual code for the Python runnable upgradeplugins

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


    def _upgrade_plugins(self):
        """x"""
        
        for plugin_info in self._client.list_plugins():
            plugin_id = plugin_info['id']

            #if plugin_id in plugins_to_skip_update:
             #   continue

            plugin_handle = self._client.get_plugin(plugin_id)

            try:
                print(f'Attempting to update plugin {plugin_id} ... ', end="")
                future = plugin_handle.update_from_store()
                future.wait_for_result()

            except Exception as e:
                print(f"Failed to update {plugin_id}: {str(e)}")



    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        # raise Exception("unimplemented")
        self.run()
        return None