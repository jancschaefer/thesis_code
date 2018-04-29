#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 08:47:14 2018

warning: this takes quite some time. to check the works, you might choose to use only a slice of the data

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

# %% Conversion Function

def workDataFrame (currentData):

	#create empty data frame for working
	converted = pd.DataFrame(columns=range(0,len(currentData.columns) + 300))
	df = pd.DataFrame(columns=range(0,300)) # vectors
	df = df.add_prefix('TE_') # prefix for vectors
	columns = currentData.columns # naming
	columns = columns.append(df.columns) # combine
	converted.columns = columns # set names

	for index, firm in currentData.iterrows():
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
		converted.loc[index] = currentData.loc[index].append(df.mean())

	return converted

# %% Prep Data
print('starting')
import multiprocessing

#df = data.loc[1:100].copy() # only 100
df = data.copy() # all of them

# create as many processes as there are CPUs on your machine
num_processes = multiprocessing.cpu_count() - 1
print(num_processes)
# calculate the chunk size as an integer
chunk_size = int(df.shape[0]/num_processes)

# this solution was reworked from the above link.
# will work even if the length of the dataframe is not evenly divisible by num_processes
chunks = [df.loc[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
print('chunked')
# %% actually do it

# create our pool with `num_processes` processes
pool = multiprocessing.Pool(processes=num_processes)

# apply our function to each chunk in the list
result = pool.map(workDataFrame, chunks)

# %% combine

#create empty data frame for working
converted = pd.DataFrame(columns=range(0,len(data.columns) + 300))
df = pd.DataFrame(columns=range(0,300)) # vectors
df = df.add_prefix('TE_') # prefix for vectors
columns = data.columns # naming
columns = columns.append(df.columns) # combine
converted.columns = columns # set names

for i in result:
   # since result[i] is just a dataframe
   # we can reassign the original dataframe based on the index of each chunk
   converted = converted.append(i)


# %% write back to disk

pq.write_table(pa.Table.from_pandas(converted), dataPath + '/converted.parquet')
converted.sample(1000).to_excel(dataPath + 'converted.xlsx')












