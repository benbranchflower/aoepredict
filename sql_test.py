# -*- coding: utf-8 -*-
"""
Created on Sat May  2 19:41:21 2020

@author: bbranchf
"""

import pandas as pd
import psycopg2

connection = psycopg2.connect("dbname=mgzdb user=burlap password=qSpV4DoFVZP4 host=aocrecs.com")   

df = pd.read_sql("SELECT * FROM matches LIMIT 10", connection)
