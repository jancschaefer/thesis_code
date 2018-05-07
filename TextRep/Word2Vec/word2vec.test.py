# -*- coding: utf-8 -*-
import sys
import pprint
from scipy import stats
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

# %% w2v

from gensim.models import Word2Vec
model = Word2Vec.load(dataPath + 'word2vec.model')

# %% testing // sanity checks

pp = pprint.PrettyPrinter(indent=4)

print('----------------- Similar to real,estate -----------------')
pp.pprint(model.wv.most_similar(['real','estate']))

print('----------------- Similar to cattle -----------------')
pp.pprint(model.wv.most_similar(['cattle']))

print('----------------- Similar to grocery -----------------')
pp.pprint(model.wv.most_similar(['grocery']))

print('----------------- Similar to gas1oline -----------------')
pp.pprint(model.wv.most_similar(['gasoline']))

print('----------------- vector for gasoline -----------------')
pp.pprint(model.wv['gasoline'])
pp.pprint(stats.describe(model.wv['gasoline']))
