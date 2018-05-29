# -*- coding: utf-8 -*-

# %% do it

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SQLContext
from pyspark.sql.types import *

from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Word2Vec
from pyspark.sql import Row

import pandas as pd

# %% init spark

sc = SparkContext("local")
spark = SparkSession(sc)
sqlCtx = SQLContext(sc)

# %% read file

sentences = pd.read_csv("02_Data/sentences.csv", header=None)
sentences = sentences[0].astype(str)
sentences.colums = ["sentence"]

sentenceDataFrame = spark.createDataFrame(sentences, StringType())
sentenceDataFrame = sentenceDataFrame.select(col("value").alias("sentence"))

# %% tokenize

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
# alternatively, pattern="\\w+", gaps(False)

countTokens = udf(lambda words: len(words), IntegerType())

tokenized = tokenizer.transform(sentenceDataFrame)
tokenized.select("sentence", "words").withColumn(
    "tokens", countTokens(col("words"))
).show(truncate=False)

regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("sentence", "words").withColumn(
    "tokens", countTokens(col("words"))
).show(truncate=False)

# %% stopwords

remover = StopWordsRemover(inputCol="words", outputCol="filtered")
tokenized = remover.transform(tokenized)
tokenized.show(n=15, truncate=80)

# %% w2v
word2Vec = Word2Vec(
    vectorSize=30,
    minCount=0,
    inputCol="filtered",
    outputCol="vector",
    windowSize=5,
    numPartitions=5,
    maxIter=5,
)
model = word2Vec.fit(tokenized)

# %% checks
l = [(["cattle"])]
rdd = sc.parallelize(l)
cattle = rdd.map(lambda x: Row(filtered=x))
cattle = sqlCtx.createDataFrame(cattle)
cattle = model.transform(cattle).select(col("vector")).toPandas()
cattle = cattle["vector"][0]

l = [(["pig"])]
rdd = sc.parallelize(l)
pig = rdd.map(lambda x: Row(filtered=x))
pig = sqlCtx.createDataFrame(pig)
pig = model.transform(pig).select(col("vector")).toPandas()
pig = pig["vector"][0]

# %% sim

from numpy import dot
from numpy.linalg import norm

cos_sim = dot(pig, cattle) / (norm(pig) * norm(cattle))
