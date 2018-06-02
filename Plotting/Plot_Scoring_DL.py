# -*- coding: utf-8 -*-

import pandas as pd
from Code.environment import filePath

# %% import data

dfx = pd.read_csv(filePath + "/01_Analysis/DL/Scoring_DL_Text.tsv", sep="\t")


def formatdf(df):
    df = df.loc[1:].copy()
    df["training_rmse"] = pd.to_numeric(df["training_rmse"])
    df["training_logloss"] = pd.to_numeric(df["training_logloss"])
    df["training_classification_error"] = pd.to_numeric(
        df["training_classification_error"]
    )
    df["validation_rmse"] = pd.to_numeric(df["validation_rmse"])
    df["validation_logloss"] = pd.to_numeric(df["validation_logloss"])
    df["validation_classification_error"] = pd.to_numeric(
        df["validation_classification_error"]
    )
    return df


dfx = formatdf(dfx)

import matplotlib.pyplot as plt

# %%

color = "RdYlGn"
figsize = [7, 5]

fig, ax = plt.subplots(1, 1)


# axarr[0].get_shared_y_axes().join(axarr[1])
# axarr[0] = axarr[0].twiny()


# axarr[0].right_ax.set_ylim(0,1)
# axarr[1].right_ax.set_ylim(0,1)


dfx.plot(
    ax=ax,
    x="iterations",
    y=[
        "training_logloss",
        "validation_logloss",
        "validation_classification_error",
        "training_classification_error",
    ],
    secondary_y=["validation_classification_error", "training_classification_error"],
    colormap=color,
    kind="line",
    title="Deep Learning with Text",
    legend=True,
    figsize=figsize,
)


ax.set_ylim(0, 20)
ax.right_ax.set_ylim(0, 1)
ax.set_ylabel("Logloss")
ax.set_xlabel("Deep Learning iterations")
fig.axes[1].set_ylabel("Error Rate")

fig.tight_layout()


# %%
fig.savefig(
    filePath + "/04_Tex/images/00_Plots/dl_scoring_history.pdf", bbox_inches="tight"
)
