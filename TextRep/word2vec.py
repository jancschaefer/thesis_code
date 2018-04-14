# -*- coding: utf-8 -*-

import sys
import pyarrow.parquet as pq
import pandas as pd

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
data = pq.read_table(dataPath + '/data.clean.parquet').to_pandas()
tradeEnglish = pd.Series(data.loc[:, 'Trade_English'])

# %% prepare for word2vec

tradeEnglish2 = tradeEnglish.apply(lambda x: [" ".join(x).strip()])

tradeEnglish3 = []

for item in tradeEnglish2:
	tradeEnglish3.append(''.join(item))

pd.Series(tradeEnglish3).to_csv(dataPath + 'sentences.csv',header=0,index=0);

# %% w2v
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
from gensim.models import word2vec
sentences = word2vec.LineSentence(dataPath + 'sentences.csv')

model = word2vec.Word2Vec(sentences,
						  min_count=10,
						  workers=4,
						  size = 300,
						  window = 5,
						  iter = 50
					  )


model.save(dataPath + 'word2vec.model')