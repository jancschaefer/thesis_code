# -*- coding: utf-8 -*-

import sys
import pyarrow as pa
import pyarrow.parquet as pq


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

logger.info('WD is set to   ' + filePath)
dataPath = (filePath + '/02_Data/')
logger.info('Writing to	 ' + dataPath)

# %% Import Data

logger.info('Reading Parquet File')
data = pq.read_table(dataPath + '/converted.parquet').to_pandas()

# %% Drop Columns
data = data.drop(
	[	"Trade_English",
		"Trade_Original",
		"nace1",
		"nace2",
		"Company_Name",
		"BvD_ID",
		"NAICS2017_primary",
		"NAICS2017_secondary",
		"GUO_Name",
		"GUO_BvDID",
		"NACE_primarydescription",
		"NACE_secondarydescription",
		"NACE_secondarycode",
		"Run", # tecnical column
		"engineUsed", # tecnical column
		"bvdind", # duplicate
		"LastError", # tecnical column
		"countryiso" # duplicate of Country_Iso
	],1)

# %% Drop Rows without >>NACE_primarycode<<

data = data.dropna(axis='index',subset=['NACE_primarycode'])

# %% Write ML Ready
pq.write_table(pa.Table.from_pandas(data), dataPath + '/converted.dropped.parquet')