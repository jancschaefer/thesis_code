library(h2o)
h2o.connect('localhost',port = 54444)

converted <- h2o.getFrame('converted_dropped_parquet.hex')
converted <- h2o.na_omit(converted)