# -*- coding: utf-8 -*-

# %% imports

import sys, os
import pyarrow.parquet as pq
import pandas as pd
import multiprocessing

print(sys.path)
print(os.getcwd())

from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

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
data = pq.read_table(dataPath + "/converted.clean.parquet").to_pandas()

# %% reduce data for testing purposes

data = data.sample(250000)


# %% split data into X and y
X = data.loc[:, data.columns != "NACE_primarycode"].copy()
Y = data.loc[:, data.columns == "NACE_primarycode"].copy()

# %% category encoding
Y["NACE_primarycode"] = Y["NACE_primarycode"].astype("int").astype("category")

X["Country_Iso"] = X["Country_Iso"].astype("category")  # .cat.codes
X["Bvd_Independence"] = X["Bvd_Independence"].astype("category")  # .cat.codes

# create dummies
XD = pd.get_dummies(
    X,
    prefix=["CI", "BVD"],
    prefix_sep="_",
    columns=["Country_Iso", "Bvd_Independence"],
    dummy_na=True,
)


# %% train/test split
seed = 7
test_size = 0.33
X_train, X_test, y_train, y_test = train_test_split(
    XD, Y, test_size=test_size, random_state=seed
)
eval_set = [(X_train, X_test), (y_train, y_test)]

# %% training xgboost

# fit model no training data
model = XGBClassifier(
    # 	silent=False,
    verbose_eval=True,
    max_depth=25,
    nthread=(multiprocessing.cpu_count() - 1),
    n_estimators=1000,
    eval_set=eval_set,
)
model.fit(X_train, y_train)

print(model)

# %% make predictions for test data
y_pred = model.predict(X_test)
predictions = [round(value) for value in y_pred]


# %% evaluate predictions
accuracy = accuracy_score(y_test, predictions)
print("Accuracy: %.2f%%" % (accuracy * 100.0))
