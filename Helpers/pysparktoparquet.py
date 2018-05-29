# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

# %% imports

import sys, os
import pandas as pd

print(sys.path)
print(os.getcwd())
# %% Logging Setup

try:
    from Code.dolog import logger
    from Code.environment import filePath
except:
    try:
        from dolog import logger
        from environment import filePath
    except:
        sys.exit("Could not import necessary Code blocks.")

logger.info("WD is set to   " + filePath)
dataPath = filePath + "/02_Data/"
logger.info("Writing to	 " + dataPath)

# %% Import Data

logger.info("Reading Parquet File")
data = pd.read_parquet(dataPath + "/data.vectorized.1000.parquet")

# %%
