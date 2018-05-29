# -*- coding: utf-8 -*-

import pandas as pd
from Code.environment import filePath

# %% import data

dfx = pd.read_csv(filePath + "/01_Analysis/DRF_Text/Scoring_DRF_Text.tsv", sep="\t")
dfnt = pd.read_csv(
    filePath + "/01_Analysis/DRF_Textless/Scoring_DRF_NoText.tsv", sep="\t"
)


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
dfnt = formatdf(dfnt)


import matplotlib.pyplot as plt

# %%

color = "RdYlGn"
figsize = [10, 5]

f, axarr = plt.subplots(1, 2, sharey="row", sharex=True)


# axarr[0].get_shared_y_axes().join(axarr[1])
# axarr[0] = axarr[0].twiny()


# axarr[0].right_ax.set_ylim(0,1)
# axarr[1].right_ax.set_ylim(0,1)


dfx.plot(
    ax=axarr[0],
    x="number_of_trees",
    y=[
        "training_logloss",
        "validation_logloss",
        "validation_classification_error",
        "training_classification_error",
    ],
    secondary_y=["validation_classification_error", "training_classification_error"],
    colormap=color,
    kind="line",
    title="DRF with Text",
    legend=False,
    figsize=figsize,
)

dfnt.plot(
    ax=axarr[1],
    x="number_of_trees",
    y=[
        "training_logloss",
        "validation_logloss",
        "validation_classification_error",
        "training_classification_error",
    ],
    secondary_y=["validation_classification_error", "training_classification_error"],
    colormap=color,
    kind="line",
    title="DRF without Text",
    legend=False,
    figsize=figsize,
)


axarr[0].set_xlabel("")
axarr[1].set_xlabel("")


axarr[0].set_ylim(0, 20)
axarr[1].set_ylim(0, 20)
axarr[0].right_ax.set_ylim(0, 1)
axarr[1].right_ax.set_ylim(0, 1)

# axarr[0].grid('on')
# axarr[1].grid('on')

axarr[1].get_yaxis().set_visible(True)  # Hide Ticks

f.text(0.5, 0.00, "Number of Trees built for both models", ha="center", va="center")

handles, labels = [], []

for h, l in zip(*f.axes[0].get_legend_handles_labels()):
    handles.append(h)
    labels.append(l)
for h, l in zip(*f.axes[2].get_legend_handles_labels()):
    handles.append(h)
    l = l + " (right)"
    labels.append(l)

axarr[0].set_ylabel("Logloss")
f.axes[3].set_ylabel("Error Rate")

ticklabels = [item.get_text() for item in f.axes[2].get_yticklabels()]

empty_string_labels = [""] * len(ticklabels)
f.axes[2].set_yticklabels(empty_string_labels)

f.legend(
    handles,
    labels,
    # 		title="Metrics",
    ncol=2,
    bbox_to_anchor=(0.93, 1.02),
    loc=4,
    borderaxespad=0.,
)

f.subplots_adjust(
    left=-0.5, bottom=None, right=-0.1, top=None, wspace=None, hspace=None
)
f.tight_layout()


# %%
f.savefig(filePath + "/04_Tex/images/00_Plots/scoring_history.pdf", bbox_inches="tight")
