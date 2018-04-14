library(h2o)
h2o.init(nthreads = -1)

data <- read.csv('./02_Data/sentences.csv')[2]
data[1] <- toString(data[1])
