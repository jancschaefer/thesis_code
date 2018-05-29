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
import os
import numpy
import inspect
from gensim.models.fasttext import FastText as FT_gensim
import nltk
from nltk.stem import WordNetLemmatizer  # working with word stems

# tokenizer
from nltk.corpus import stopwords
from sklearn.feature_extraction import text

print("starting on %s", platform.node())

# %% Logging Setup

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

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

logger.info("Reading Parquet File")
data = pq.read_table(dataPath + "/data.clean.parquet").to_pandas()

# %% load w2v model

logger.info("Loading Model")
model = FT_gensim.load(dataPath + "fasttext.model")

# %% Conversion Function


def workDataFrame(currentData):

    # create empty data frame for working
    converted = pd.DataFrame(columns=range(0, len(currentData.columns) + 300))
    df = pd.DataFrame(columns=range(0, 300))  # vectors
    df = df.add_prefix("DESC_")  # prefix for vectors
    columns = currentData.columns  # naming
    columns = columns.append(df.columns)  # combine
    converted.columns = columns  # set names

    for index, firm in currentData.iterrows():  # for every single company
        print("Working on %s", index)
        logger.info("(%s) Working on: %s", index, firm["NACE_Code"])
        NaceDescription = firm["Description"]  # Extract Trade Description
        df = pd.DataFrame(columns=range(0, 300))  # Empty Frame for vectors
        df = df.add_prefix("DESC_")  # add prefix

        for word in NaceDescription:  # for every word => build vector
            try:
                df = df.append(
                    pd.Series(model.wv[word], index=df.columns), ignore_index=True
                )  # append every vector to df
            except:
                continue
        logger.info(
            "(%s) Working on: %s => %s entries", index, firm["NACE_Code"], len(df)
        )
        converted.loc[index] = currentData.loc[index].append(
            df.mean()
        )  # calculate mean per company and add to converted

    return converted  # return converted to the pool function


# %% Prep Data

nace1 = data[["NACE_primarycode", "NACE_primarydescription"]]
nace2 = data[["NACE_secondarycode", "NACE_secondarydescription"]]

columns = ["NACE_Code", "Description"]
nace1.columns = nace2.columns = columns

nace_codes = nace1.append(nace2).drop_duplicates().sort_values("NACE_Code")
nace_codes.loc[:, "Description"].str.lower()
nace_codes.loc[:, "Description"].str.strip()

# %% prepare stopwords and other language processing

nltk.download("stopwords")  # used to remove common words without meaning, e.g. "and"

stop1 = set(text.ENGLISH_STOP_WORDS)  # Load stopwords
stop2 = set(stopwords.words("english"))  # load further stopwords
stop = stop1 | stop2

# %% tokenizing the data

nace_codes.loc[:, "Description"] = nace_codes.loc[:, "Description"].astype(str)
nace_codes.loc[:, "Description"] = (
    nace_codes.loc[:, "Description"].str.lower().str.split()
)  # split every trade description into single words
nace_codes.loc[:, "Description"] = nace_codes.loc[:, "Description"].apply(
    lambda x: [item for item in x if item not in stop]
)  # for every cell remove stop words

# %% working with word stems

nltk.download("wordnet")  # used for working with word stems

lem = WordNetLemmatizer()  # create lemmatizer

nace_codes.loc[:, "Description"] = nace_codes.loc[:, "Description"].apply(
    lambda x: [lem.lemmatize(item) for item in x]
)  # reduce words to stems; e.g. products => product

# %% reset index and remove NAs
nace_codes = nace_codes.reset_index()
nace_codes = nace_codes.dropna(axis="index", subset=["NACE_Code"])

# %% apply model
nace_codes = nace_codes[["NACE_Code", "Description"]]
nace_worked = workDataFrame(nace_codes)

# %% write xls
nace_worked.to_excel(dataPath + "nacecodes.ft.xlsx")
pq.write_table(pa.Table.from_pandas(nace_worked), dataPath + "/nacecodes.ft..parquet")

# %% check


def getVector(item, prefix="DESC"):

    filter_col = [col for col in nace_worked if col.startswith(prefix)]
    vector = item[filter_col]

    return vector


def vecDistance(item1, item2, prefix="DESC"):
    vec1 = getVector(item1)
    vec2 = getVector(item2)
    return numpy.linalg.norm(vec1 - vec2)


item1 = nace_worked.loc[793]
item2 = nace_worked.loc[794]

print(vecDistance(item1, item2, prefix="DESC"))

item1 = nace_worked.loc[668]
item2 = nace_worked.loc[666]
print(vecDistance(item1, item2, prefix="DESC"))
