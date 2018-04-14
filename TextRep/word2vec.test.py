# -*- coding: utf-8 -*-
import sys
import pprint
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

pp = pprint.PrettyPrinter(indent=4)

print('----------------- Similar to real,estate -----------------')
pp.pprint(model.wv.most_similar(['real','estate']))

print('----------------- Similar to cattle -----------------')
pp.pprint(model.wv.most_similar(['cattle']))

print('----------------- Similar to grocery -----------------')
pp.pprint(model.wv.most_similar(['grocery']))

print('----------------- Similar to gasoline -----------------')
pp.pprint(model.wv.most_similar(['gasoline']))