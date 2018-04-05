#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  5 13:32:49 2018

@author: janschaefer
"""

import os

# %% set up environment

try:
    filePath = __file__
    head, tail = os.path.split(filePath)
    filePath = head + '/..'
except:
    filePath = os.getcwd()

if (filePath == '/..'):
    head, tail = os.path.split(os.getcwd())
    filePath = head + '/..'
    filePath = os.getcwd() + '/..'

os.chdir(filePath)
filePath = os.getcwd()

print(os.getcwd())
print(filePath)
