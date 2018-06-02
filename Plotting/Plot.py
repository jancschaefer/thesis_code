# -*- coding: utf-8 -*-

# %% imports

import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pandas.plotting import table

print(sys.path)
print(os.getcwd())


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
data = pq.read_table(
    dataPath + "/data.clean.nodups.parquet"
).to_pandas()  # -*- coding: utf-8 -*-
nace_table = pq.read_table(
    dataPath + "/nacecodes.w2v.parquet"
).to_pandas()  # -*- coding: utf-8 -*-

da  # %% basic stats

nace = data["NACE_primarycode"].astype(str)

nacecounts = nace.value_counts()

nacex = [nacecounts, (nacecounts / len(nace) * 100).round(decimals=2)]
nacex = pd.DataFrame(nacex).transpose()
nacex.columns = ["Count", "Percentage"]
nacex["Count"].describe()

pd.DataFrame(nacex["Count"].describe()).to_csv("01_Analysis/nace_distribution.csv")
nacex.to_csv("01_Analysis/nace_counts.csv")
nacex.to_excel("01_Analysis/nace_counts.xlsx", sheet_name="NaceCounts")

# %% get Descriptions
nacedesc = nace_table.iloc[:, 0:2]
nacedesc.index = nacedesc["NACE_Code"]
nacex["NACE_Code"] = nacex.index
nacedesc["NACE_Code"] = nacedesc["NACE_Code"].astype(str)
nacex["NACE_Code"] = nacex["NACE_Code"].astype(str)
naceall = pd.merge(nacex, nacedesc, on="NACE_Code")
naceall.index = naceall["NACE_Code"]
naceall = naceall.drop("NACE_Code", axis=1)


def zxx(x):
    x = np.array2string(x)
    x = x[1 : (len(x) - 1)][0:60]
    return x


naceall["Description"] = naceall["Description"].map(zxx)

# %% plot

# from matplotlib import rcParams
# rcParams['font.family'] = 'Helvetica'

# %% plot distribution
n = 20
min = 100

plot = nacex.plot(y="Count")
plot.get_figure().savefig(filePath + "/05_Plots/nace_distribution.pdf")

# %% plot num of companies per category

plot2 = nacex["Count"].hist()
plot2.get_figure().savefig(filePath + "/05_Plots/nace_distribution2.pdf")

# %% plot combined


fig, ax = plt.subplots(1, 1)
ax.get_xaxis().set_visible(False)  # Hide Ticks

nacefortable = naceall["Count"]
nacefortable.name = "NACE_Code"

table1 = table(
    ax,
    np.round(nacefortable.describe(), 2),
    loc="upper right",
    colLoc="center",
    colWidths=[0.3, 0.5],
)

# ttable = table(ax, np.round(naceall.head(15).append(naceall.tail(15)), 2),
# 			   colWidths=[0.1, 0.1, 0.8],
# 			   colLoc='center',
# 			   loc="bottom")

plot = (
    naceall.plot(
        lw=2,
        kind="area",
        alpha=0.9,
        colormap="gray",
        legend=False,
        figsize=(8, 4),
        y="Count",
        # 		title="Number of companies per NACE_Code",
        ax=ax,
    )
    .get_figure()
    .tight_layout()
)

ax.set_ylabel("Number of firms per NACE Code")
ax.set_xlabel("NACE Code arranged by number of firms")

plt.text(
    0.98,
    0.35,
    "number of firms = " + str(len(data.index)),
    horizontalalignment="right",
    transform=ax.transAxes,
)
plt.text(
    0.98,
    0.30,
    "codes with less than 1000 firms = " + str(len(naceall[naceall["Count"] < 1000])),
    horizontalalignment="right",
    transform=ax.transAxes,
)
plt.text(
    0.98,
    0.25,
    "codes with less than 100 firms = " + str(len(naceall[naceall["Count"] < 100])),
    horizontalalignment="right",
    transform=ax.transAxes,
)
plt.text(
    0.98,
    0.20,
    "codes with less than 10 firms = " + str(len(naceall[naceall["Count"] < 10])),
    horizontalalignment="right",
    transform=ax.transAxes,
)

plt.show()

# %% write
fig.savefig(
    filePath + "/04_Tex/images/00_Plots/nace_summary_short.pdf", bbox_inches="tight"
)
