# -*- coding: utf-8 -*-

# %%
import pandas as pd

cm = pd.read_clipboard()


def getLast(x):
    x = "000000" + x
    return x[-4:]


cm["NACE"] = cm["NACE"].map(getLast)
cm = cm.sort_values("NACE")

cmt = cm.T
cmt.columns = cmt.iloc[0]
cmt = cmt[1:]

cmt["NACE"] = cmt.index
cmt["NACE"] = cmt["NACE"].map(getLast)

cmt = cmt.sort_values("NACE")

cm = cmt.T
cm = cm.drop(["NACE", "otal"])
cm = cm[cm.columns[:-2]]

cm.to_clipboard(sep="\t")
