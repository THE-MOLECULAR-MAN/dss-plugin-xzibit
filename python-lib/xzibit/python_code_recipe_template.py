# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu


# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

unaltered_default_python_code_recipe_df = (
    ...
)  # Compute a Pandas dataframe to write into unaltered_default_python_code_recipe


# Write recipe outputs
unaltered_default_python_code_recipe = dataiku.Dataset(
    "unaltered_default_python_code_recipe"
)
unaltered_default_python_code_recipe.write_with_schema(
    unaltered_default_python_code_recipe_df
)
