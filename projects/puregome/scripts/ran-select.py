#!/usr/bin/env python3
"""
    ran-select.py: randomly select Dutch tweets from json file
    usage: gunzip -c file.json.gz | python3 ran-select.py
    20200507 erikt(at)xs4all.nl
"""

import json
import random
import sys

DUTCH = "nl"
LANG = "lang"
SAMPLESIZE = 0.02

for line in sys.stdin:
    line = line.strip()
    jsonData = json.loads(line)
    tweetLang = jsonData[LANG]
    if tweetLang == DUTCH and random.random() < SAMPLESIZE:
        print(line)
