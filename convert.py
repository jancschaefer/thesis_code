#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 08:26:45 2018

@author: janschaefer
"""
import sys
 
def convertGerman(text):
    
    text = text.replace('\x8a','ä')
    text = text.replace('\x86','Ü')
    text = text.replace('\x85','Ö')
    text = text.replace('\x80','Ä')
    text = text.replace('\x9f','ü')
    text = text.replace('\x9a','ö')
    text = text.replace('§','ß')
    return text

def convertFrench(text):
    text = text.replace('\x8e','é')
    text = text.replace('\x8f','è')
    text = text.replace('\x8d','č')
    text = text.replace('\x90','ê')
    text = text.replace('\x88','')
    return text

def convertSwedish(text):
    text = text.replace('\x8c','Œ')
    return text

def convert(text):
        try:
            text = text.group(0).encode('latin1').decode('utf8')
        except:
            try:
                text = text.group(0)
            except:
                return text
        return text
        
def remove_non_ascii_1(text):
    return text.encode(sys.stdout.encoding, 'ignore').decode(sys.stdout.encoding)

def convertBrokenText(text):
    text = convertGerman(text)
    text = convertFrench(text)
    text = convertSwedish(text)
    
    text = convert(text)
    text = remove_non_ascii_1(text)
    
    
    return text