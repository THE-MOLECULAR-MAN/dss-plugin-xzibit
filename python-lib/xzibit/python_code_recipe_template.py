# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu


# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

TEMPLATE_df = ...  # Compute a Pandas dataframe to write into TEMPLATE


# Write recipe outputs
TEMPLATE = dataiku.Dataset("TEMPLATE")
TEMPLATE.write_with_schema(TEMPLATE_df)
