# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
INPUT_PLACEHOLDER = dataiku.Dataset("INPUT_PLACEHOLDER")
INPUT_PLACEHOLDER_df = INPUT_PLACEHOLDER.get_dataframe()




# Write recipe outputs
OUTPUT_PLACEHOLDER = dataiku.Folder("PLACEHOLDER3")
OUTPUT_PLACEHOLDER_info = OUTPUT_PLACEHOLDER.get_info()
