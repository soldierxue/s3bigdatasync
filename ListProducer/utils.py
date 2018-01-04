#!/usr/bin/python
# -*- coding: utf8 -*-


from pprint import pprint

import datetime
import hmac
import hashlib
import json

def load_json_from_file(filename):
    json_data = open(filename).read()
    data = json.loads(json_data)
    return data
