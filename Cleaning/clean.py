# -*- coding: utf-8 -*-

import sys
import pyarrow as pa
import pyarrow.parquet as pq
import nltk
from nltk.stem import WordNetLemmatizer  # working with word stems

# tokenizer
from nltk.corpus import stopwords
from sklearn.feature_extraction import text

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

logger.info("Reading Parquet File")
data = pq.read_table(dataPath + "/data.parquet").to_pandas()

# %% Clean Data with pandas
withETD = data[
    data["Trade_English"] != ""
].copy()  # Remove rows with empty trade descriptions :: ETD ^= English Trade Description

withETD.loc[:, "Trade_English"] = withETD.loc[:, "Trade_English"].astype(str)

withETD.loc[:, "Trade_English"] = withETD.loc[:, "Trade_English"].str.replace(
    "Registered item:", ""
)  # Remove registered item for german companies
withETD.loc[:, "Trade_English"] = withETD.loc[:, "Trade_English"].str.replace(
    "ivnost:", ""
)  # Remove ivnost for chech companies
withETD.loc[:, "Trade_English"] = withETD.loc[:, "Trade_English"].str.replace(
    r"([\:\?\,\.\|\(\)\"\'\\]\s*){1,}", " "
)  # Remove special characters
withETD.loc[:, "Trade_English"] = withETD.loc[:, "Trade_English"].str.replace(
    r"([\s+])", " "
)  # Remove multiple whitespaces
withETD.loc[:, "Trade_English"] = withETD.loc[:, "Trade_English"].str.replace(
    r"[^A-Za-z\s]+", ""
)  # only keep text

withETD.loc[:, "Trade_English"] = withETD.loc[
    :, "Trade_English"
].str.lower()  # Convert all text to lowercase
withETD.loc[:, "Trade_English"] = withETD.loc[
    :, "Trade_English"
].str.strip()  # Remove leading and tailing spaces

withETD = withETD[
    withETD["Trade_English"] != ""
].copy()  # Remove remaining rows with empty trade descriptions

# %% prepare stopwords and other language processing

nltk.download("stopwords")  # used to remove common words without meaning, e.g. "and"

stop1 = set(text.ENGLISH_STOP_WORDS)  # Load stopwords
stop2 = set(stopwords.words("english"))  # load further stopwords
stop = stop1 | stop2

# %% tokenizing the data

tradeEnglish = withETD.loc[:, "Trade_English"]

tradeEnglish = (
    tradeEnglish.str.lower().str.split()
)  # split every trade description into single words
tradeEnglish = tradeEnglish.apply(
    lambda x: [item for item in x if item not in stop]
)  # for every cell remove stop words

# %% working with word stems

nltk.download("wordnet")  # used for working with word stems

lem = WordNetLemmatizer()  # create lemmatizer

tradeEnglish = tradeEnglish.apply(
    lambda x: [lem.lemmatize(item) for item in x]
)  # reduce words to stems; e.g. products => product

# %% Join back with main data
withETD.loc[:, "Trade_English"] = tradeEnglish

# %% Exploratory

# Get number of words for every company. This text quite some time as it is executing on only one core.

index = 0
numwords = []
withETD.loc[:, "numwords"] = 0
for index, row in withETD.iterrows():
    numwords.append(len(row["Trade_English"]))

withETD.loc[:, "numwords"] = numwords


# %% write clean data to disk
pq.write_table(pa.Table.from_pandas(withETD), dataPath + "/data.clean.parquet")
withETD.loc[:, "Trade_English"].to_csv(dataPath + "/words.txt")
