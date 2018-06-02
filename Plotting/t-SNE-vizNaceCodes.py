# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import pandas as pd
import sys

# %% set data path etc:

try:
    from Code.environment import filePath
except:
    try:
        from environment import filePath

    except:
        sys.exit("Could not import necessary Code blocks.")
dataPath = filePath + "/02_Data"

# %%
def breakArray(x):
    x = ",".join(x)
    x = str.replace(",,", ",", x)
    return x


def getCategory(x):
    x = round(x, 0)
    x = str(x)
    x = x.replace(".0", "")
    x = "0000" + str(x)
    x = x[-4:][0]
    return int(x)


# %% export for third party analysis like:  Embedding projector - https://projector.tensorflow.org

nace = pd.read_parquet(dataPath + "/nacecodes.w2v.parquet")
nace.iloc[:, 2:].to_csv(dataPath + "/naceViz.tsv", sep="\t", index=False, header=False)

nace["Description"] = nace["Description"].map(breakArray)

naceexport = nace
naceexport["Category"] = naceexport["NACE_Code"].map(getCategory)
naceexport[["NACE_Code", "Description", "Category"]].replace("\n", " ").to_csv(
    dataPath + "naceVizmeta.tsv", sep="\t", index=False, header=True
)

# %% pca

from sklearn.decomposition import PCA

pca = PCA(n_components=3)

pca_result = pca.fit_transform(nace.iloc[:, 2:].values)

nace["pca-one"] = pca_result[:, 0]
nace["pca-two"] = pca_result[:, 1]
nace["pca-three"] = pca_result[:, 2]

# %% tsne

import time
from sklearn.manifold import TSNE

time_start = time.time()
tsne = TSNE(n_components=2, verbose=1, perplexity=50, n_iter=6000)
tsne_results = tsne.fit_transform(nace.iloc[:, 2:-1].values)

print("t-SNE done! Time elapsed: {} seconds".format(time.time() - time_start))

# %% plot
tsnedf = pd.DataFrame(tsne_results)

nace["tsne1"] = tsnedf[0]
nace["tsne2"] = tsnedf[1]
nace["tsne3"] = tsnedf[1]
nace["First_Digit"] = naceexport["Category"]

# %% plot matplotlib
fig, ax = plt.subplots(1, 1)

plot = nace.plot(
    ax=ax,
    kind="scatter",
    x="tsne1",
    y="tsne2",
    title="NACE Codes clustered using t-SNE.",
    c="First_Digit",
    cmap=plt.cm.get_cmap("viridis", 10),
    figsize=(12, 8),
)

ax.set_ylabel("t-SNE Component 1")
ax.set_xlabel("t-SNE Component 2")


fig.tight_layout()
# %% save
fig.savefig(filePath + "/04_Tex/images/00_Plots/t-SNE.pdf", bbox_inches="tight")

# %% plot matplotlib 3d

time_start = time.time()
tsne = TSNE(n_components=3, verbose=1, perplexity=50, n_iter=6000)
tsne_results = tsne.fit_transform(nace.iloc[:, 2:-1].values)

print("t-SNE done! Time elapsed: {} seconds".format(time.time() - time_start))

tsnedf = pd.DataFrame(tsne_results)

# %% plot3d

from mpl_toolkits import mplot3d

nace3d = nace
nace3d["tsne1"] = tsnedf[0]
nace3d["tsne2"] = tsnedf[1]
nace3d["tsne3"] = tsnedf[1]
nace3d["First_Digit"] = naceexport["Category"]

fig, ax = plt.subplots()
ax = plt.axes(projection="3d")
ax.scatter3D(
    nace3d["tsne1"],
    nace3d["tsne2"],
    nace3d["tsne3"],
    c=nace3d["First_Digit"],
    cmap="viridis",
)
ax.set_xlabel("t-SNE Component 1")
ax.set_ylabel("t-SNE Component 2")
ax.set_zlabel("t-SNE Component 3")
fig.set_size_inches(10, 10)

fig.tight_layout()
# %% save
fig.savefig(filePath + "/04_Tex/images/00_Plots/t-SNE-3d.pdf", bbox_inches="tight")
