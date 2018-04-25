#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 08:47:14 2018

@author: janschaefer
"""

# -*- coding: utf-8 -*-

import sys
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
import platform

print('starting on %s',platform.node())

# %% Logging Setup

import os,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

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
data = pq.read_table(dataPath + '/data.clean.parquet').to_pandas()

# %% load w2v model

logger.info('Loading Model')
from gensim.models import Word2Vec
model = Word2Vec.load(dataPath + 'word2vec.model')

# %% Prepare Column Names

logger.info('Starting to prepare Column Names')
converted = pd.DataFrame(columns=range(0,len(data.columns) + 300))
df = pd.DataFrame(columns=range(0,300))
df = df.add_prefix('TE_')
columns = data.columns
columns = columns.append(df.columns)
converted.columns = columns
del(columns)

# %% Convert Words

for index, firm in data.iterrows():
	print('Working on %s', index)
	logger.info('(%s) Working on: %s', index, firm['Company_Name'] )
	Trade_Description = firm['Trade_English']
	df = pd.DataFrame(columns=range(0,300))
	df = df.add_prefix('TE_')

	for word in Trade_Description:
		try:
			df = df.append(pd.Series(model.wv[word],index=df.columns), ignore_index=True)
		except:
			continue
	logger.info('(%s) Working on: %s => %s entries', index, firm['Company_Name'], len(df) )


	converted.loc[index] = data.loc[index].append(df.mean())

# %% write back to disk

pq.write_table(pa.Table.from_pandas(converted), dataPath + '/converted.parquet')
converted.sample(1000).to_excel(dataPath + 'converted.xlsx')












