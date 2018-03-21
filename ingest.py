# -*- coding: utf-8 -*-
# %% Import Block
import os, sys
#import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import deepl

# %% environment

try:
    filePath = __file__
    head, tail = os.path.split(filePath)
    filePath = head + '/..'
except:
    filePath = os.getcwd()  


if(filePath == '/..'):
    head, tail = os.path.split(os.getcwd())
    filePath = head + '/..'
    filePath = os.getcwd() + '/..' 
    
os.chdir(filePath)
filePath = os.getcwd()
    
print(os.getcwd())
print(filePath)



try:
    from Code.dolog import logger
    from Code.jsTranslate import translate
    from Code.convert import convertBrokenText
except:
    try:
        from dolog import logger
        from jsTranslate import translate
        from convert import convertBrokenText
    except:
        sys.exit("Could not import necessary Code blocks.")

logger.info('WD is set to   ' + filePath)
dataPath = (filePath + '/02_Data/')
logger.info('Writing to     ' + dataPath)
translationEngine = 'azure' ## select either azure or deepl


# %% Stuff
#if False:
#    # %% Initial data load
#    logger.info('Reading Stata File')
#    data = pd.read_stata('/Users/janschaefer/Dropbox/10_Thesis/02_Data/save_step4.dta', encoding='utf-8')
#
#    # %% writing parquet
#    logger.info('Writing parquet to disk.')
#    pq.write_table(pa.Table.from_pandas(data), dataPath+'/data.parquet')

# %% Initial data load
logger.info('Reading Parquet File')
data = pq.read_table(dataPath+'/data.parquet').to_pandas()


# %% Data Summary
#logger.info('Data with types:\n%s', data.dtypes)
logger.info('Dimensions of data: %s', data.shape)
#data = data.head(1000)


# %%  create needTranslation subset

file = open(filePath+"/iterate.txt","r") 
iterate = file.read()
file.close()
startIndex = int(iterate)
endIndex    = startIndex + 5000

logger.info('Translating ' + str(startIndex) + ' until ' + str(endIndex) + '.')

needTranslation = data.iloc[startIndex:endIndex]

needTranslation = needTranslation.loc[needTranslation['Trade_English']==""]
needTranslation = needTranslation[needTranslation['Trade_Original']!=""]
needTranslation = needTranslation.iloc[0:,[1,2,4,5]]

logger.info('%s of %s Records to be translated',needTranslation.shape[0], (endIndex-startIndex))

# %% set supported countries
countries = needTranslation['Country_Iso'].unique()
logger.info('Found Countries: %s', countries)

countriesDeepl = ['EN','DE','FR','ES','IT','NL','PL', 'CH', 'AT']
logger.info('Accepting Countries: %s', countriesDeepl)

# %% perform actual translation

i=errcount  = 0; # starting value
maxI        = 5000; # how many translations should be attempted? | 0 for unlimited
maxErrors   = 20;

logger.info('Starting translation')

for index, item in needTranslation[needTranslation['Trade_English']==""].iterrows():

    if(index%10 == 0): # Log current Index Position
        logger.info('Index now at %s', index)

    if (index < startIndex) or (index >= endIndex) or (i >= maxI and maxI != 0) or (errcount >= maxErrors and maxErrors != 0): # in case it runs out of bounce; should not be necessary
        logger.critical('Stopping translation attempt. index %s, startIndex %s ,endIndex %s, i %s,maxI = %s, errcount = %s,maxErrors =  %s,',index,startIndex,endIndex,i,maxI,errcount,maxErrors)
        break;

    if (item['Country_Iso'] in countriesDeepl) == False and translationEngine == 'deepl': # Only translate languages that are supported, when deepl is selected
        data.loc[index,"LastError"] = "ERR_TRANSLATE:LANGUAGE_UNAVAILABLE"
        logger.warning(item['Country_Iso'] + ' not supported')
        continue;

    if item['Trade_Original'] == np.NaN:
        needTranslation.loc[index]['Trade_Original'] = np.NaN
        needTranslation.loc[index]['Trade_English'] = np.NaN
        continue
    else:
        text = item['Trade_Original'].strip() # clean string
        text = convertBrokenText(text)

    len(text) == 0
    if len(text) == 0:
        #needTranslation.loc[index]['Trade_Original'] = np.NaN # set to NaN if text is 0 characters long
        #needTranslation.loc[index]['Trade_English'] = np.NaN
        continue
    else:
            if (translationEngine == 'deepl'): # DEEPL Translation
                try:
                    # Translate using deepl translate API
                    translation = deepl.translate(text, target="EN")
                    data.loc[index,"Trade_English"] = translation[0] # store which engine was used
                    data.loc[index,"engineUsed"] = translationEngine
                    
                    logger.info("Translated: %s > %s | using %s",text,translation,translationEngine)
                    
                    file = open("iterate.txt","w") 
                    file.write(str(index))
                    file.close()
                    
                    i = i+1
    
                except: # if translation fails, add to errcount
                    errcount = errcount + 1
                    data.loc[index,"LastError"] = "ERR_TRANSLATE_OTHER"
                    logger.error('Could not Translate:\n%s', item)
            if (translationEngine == 'azure'): # Azure Translation
                try:
                    # Translate using deepl translate API
                    translation = translate(text, target="en-us")
                    data.loc[index,"Trade_English"] = translation
                    data.loc[index,"engineUsed"] = translationEngine # store which engine was used
                    
                    logger.info("Translated: %s > %s | using %s",text,translation,translationEngine)
                    
                    file = open("iterate.txt","w") 
                    file.write(str(index))
                    file.close()
                    
                    i = i+1
    
                except: # if translation fails, add to errcount
                    errcount = errcount + 1
                    data.loc[index,"LastError"] = "ERR_TRANSLATE_OTHER"
                    logger.error('Could not Translate:\n%s', item)

# %% writing to disk
logger.info('Writing parquet to disk.')
pq.write_table(pa.Table.from_pandas(data), dataPath+'/data.parquet')
logger.info('Writing Debug to disk.')
csvDebug = data.loc[startIndex:endIndex].to_csv(dataPath+'/debug.csv')
logger.info('Finished Writing Files.')
logger.info('Finished with index %s',index)

 # %% Stuff
#data.iloc[startIndex:startIndex + 300,0:6].to_csv(dataPath+'/debug2.csv')


# %% adding columns
#logger.info('Writing parquet to disk.')

#data['LastError'] = np.NaN
#data['engineUsed'] = np.NaN

#pq.write_table(pa.Table.from_pandas(data), dataPath+'/data_translated.parquet')