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
data = pq.read_table(dataPath + '/data.parquet').to_pandas()

# %% Clean Data with pandas
withETD = data[data['Trade_English'] != ""].copy()  # Remove rows with empty trade descriptions

withETD.loc[:,'Trade_English'] = withETD.loc[:,'Trade_English'].str.replace('Registered item:','')  # Remove registered item for german companies
withETD.loc[:,'Trade_English'] = withETD.loc[:,'Trade_English'].str.replace('ivnost:','')  # Remove ivnost for chech companies
withETD.loc[:,'Trade_English'] = withETD.loc[:,'Trade_English'].str.replace(r'([\:\?\,\.\|\(\)\"\'\\]\s*){2,}', '')  # Remove continuous special characters
withETD.loc[:,'Trade_English'] = withETD.loc[:,'Trade_English'].str.lower()  # Convert all text to lowercase
withETD.loc[:,'Trade_English'] = withETD.loc[:,'Trade_English'].str.strip()  # Remove leading and tailing spaces

withETD = withETD[withETD['Trade_English'] != ""].copy()  # Remove remaining rows with empty trade descriptions

# %% write clean data to disk
pq.write_table(pa.Table.from_pandas(withETD), dataPath + '/data.clean.parquet')