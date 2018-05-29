# -*- coding: utf-8 -*-

# %% import
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

# %% stuff

spark = (
    SparkSession.builder.master("local")
    .appName("muthootSample1")
    .config("spark.executor.memory", "10gb")
    .config("spark.cores.max", "6")
    .getOrCreate()
)

sc = spark.sparkContext

# %% read
# using SQLContext to read parquet file
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

# to read parquet file
df = sqlContext.read.parquet("/Users/janschaefer/Downloads/p.parquet")
