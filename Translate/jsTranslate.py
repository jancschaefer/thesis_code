# -*- coding: utf-8 -*-

# %% Setup Translator

import http.client, urllib.parse
import xml.etree.cElementTree as et

# **********************************************
# *** Update or verify the following values. ***
# **********************************************

# Replace the subscriptionKey string value with your valid subscription key.
subscriptionKey = "306fb3a1af9746f4a8e0b7c1906d670e"

host = "api.microsofttranslator.com"
path = "/V2/Http.svc/Translate"


def translate(text, target="en-us"):
    params = "?to=" + target + "&text=" + urllib.parse.quote(text)

    def get_suggestions():
        headers = {"Ocp-Apim-Subscription-Key": subscriptionKey}
        conn = http.client.HTTPSConnection(host)
        conn.request("GET", path + params, None, headers)
        response = conn.getresponse()
        return response.read()

    result = get_suggestions()
    # print (result.decode("utf-8"))
    root = et.fromstring(result.decode("utf-8"))

    return root.text
