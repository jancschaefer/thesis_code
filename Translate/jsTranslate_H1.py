# -*- coding: utf-8 -*-

# %% Setup Translator

import http.client, urllib.parse
import xml.etree.cElementTree as et

# **********************************************
# *** Update or verify the following values. ***
# **********************************************

# Replace the subscriptionKey string value with your valid subscription key.
subscriptionKey = '209779672b034b5ab5aed1285b3c0875'

host = 'api.microsofttranslator.com'
path = '/V2/Http.svc/Translate'

proxy = "proxy.henkelgroup.net"
port = 80

def translate(text, target = 'en-us'):
    params = '?to=' + target + '&text=' + urllib.parse.quote (text)
    
    def get_suggestions ():
        headers = {'Ocp-Apim-Subscription-Key': subscriptionKey}
        conn = http.client.HTTPSConnection(proxy,port,timeout=3)
        conn.set_tunnel(host)
        conn.request ("GET", path + params, None, headers)
        response = conn.getresponse ()
        return response.read ()
    
    result = get_suggestions ()
    #print (result.decode("utf-8"))
    root = et.fromstring(result.decode("utf-8"))
    
    return root.text