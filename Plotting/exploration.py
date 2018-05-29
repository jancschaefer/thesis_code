# -*- coding: utf-8 -*-

# %% numwords

numwords = data["numwords"].describe()
numwords = pd.DataFrame(numwords)
numwords.to_excel("01_Analysis/numwords.xlsx", sheet_name="numwords")
nw = data["numwords"]
