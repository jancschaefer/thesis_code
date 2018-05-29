# -*- coding: utf-8 -*-

import sys

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
## Note: Not necessary. Uses sentences.csv from the word2vec model

# %% Train Fasttext

from gensim.models.fasttext import FastText as FT_gensim
from gensim.models.word2vec import LineSentence

sentences = LineSentence(dataPath + "sentences.csv")

model_gensim = FT_gensim(size=300)

# build the vocabulary
model_gensim.build_vocab(sentences)

# train the model
model_gensim.train(
    sentences, total_examples=model_gensim.corpus_count, epochs=model_gensim.epochs
)

print(model_gensim)

# %% save model
model_gensim.save(dataPath + "fasttext.model")

# %% sanity check
model_gensim.most_similar("cattle")
model_gensim.most_similar(["super", "market"])
model_gensim.most_similar(["pharma"])
model_gensim.wv.doesnt_match("pharma medical pharmaceutic cattle".split())
model_gensim.wv.doesnt_match("pharma medical pharmaceutic".split())
