# -*- coding: utf-8 -*-

import sys
from gensim.models.doc2vec import Doc2Vec
from gensim.models.doc2vec import TaggedDocument
import gensim
import scipy
import multiprocessing
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
#data = pq.read_table(dataPath + '/data.clean.parquet').to_pandas() using sentences.csv

# %% initialize
documents = gensim.models.doc2vec.TaggedLineDocument(dataPath + 'sentences.csv')
model = Doc2Vec(	dm=1,
					vector_size=600,
					negative=0,
					window=5,
					min_count=5,
					alpha=0.2, min_alpha=0.025,
					max_vocab_size=None,
					sample=2000,
					epochs=500,
					workers=(multiprocessing.cpu_count()-1)*3
				)
# %% build
model.build_vocab(	documents=documents,
				  	progress_per=10000
				)
# %% train
model.train(	documents=documents,
				epochs=2000,
				total_examples=model.corpus_count,
				start_alpha=0.2, end_alpha=0.025,
			)

# %% sanity check
sentence1 = TaggedDocument(words=set([u'raise',u'of',u'dairy',u'cattle']),tags=set(['doc1']))
sentence2 = TaggedDocument(words=set([u'raise',u'of',u'pig']),tags=set(['doc2']))
sentence3 = TaggedDocument(words=set([u'repair',u'of',u'motorcycle']),tags=set(['doc3']))
sentence4 = TaggedDocument(words=set([u'repair',u'of',u'truck']),tags=set(['doc4']))

vec1 = model.infer_vector(['raise','dairy','cattle'])
vec1_1 = model.infer_vector(['raise','dairy','cattle'])
vec2 = model.infer_vector(['raise','pig','pork'])

vec3 = model.infer_vector(sentence3.words)
vec4 = model.infer_vector(sentence4.words)

print("Cattle & Cattle: \t",scipy.spatial.distance.euclidean(vec1,vec1_1)," \t| expected 0")
print("Cattle & Pig: \t\t",scipy.spatial.distance.euclidean(vec1,vec2)," \t| expected low")
print("Motorc & Trucks: \t",scipy.spatial.distance.euclidean(vec3,vec4)," \t| expected lowest")
print("Cattle & Motorcycles: \t",scipy.spatial.distance.euclidean(vec1,vec3)," \t| expected high")
print("Cattle & Trucks: \t",scipy.spatial.distance.euclidean(vec1,vec4)," \t| expected high")

# %% save
model.save(dataPath + 'doc2vec_500.model')