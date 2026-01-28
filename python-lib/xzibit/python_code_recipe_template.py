# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
default_py_recipe_input = dataiku.Dataset("default_py_recipe_input")
default_py_recipe_input_df = default_py_recipe_input.get_dataframe()




# Write recipe outputs
default_py_recipe_output = dataiku.Folder("QKXa3XKF")
default_py_recipe_output_info = default_py_recipe_output.get_info()
