# -*- coding: utf-8 -*-

import sys
import pyarrow.parquet as pq
import pyarrow as pa


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

# %% load data

data = pq.read_table(dataPath + '/data.clean.parquet').to_pandas()

# %% split data
last = 0
split = 20000

for index in range(0,20):

	pq.write_table(pa.Table.from_pandas(data.iloc[last:last+split]), dataPath + '/convert/convert_' + str(index) + '.parquet')
	last = last + split