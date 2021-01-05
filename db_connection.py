# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 10:06:39 2021

@author: salem
"""

import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey

'''Pkg might be needed'''
# import numpy as np
# import mysql.connector
# import pymysql
# import sys
# import os
# import sshtunnel
# from sshtunnel import SSHTunnelForwarder
# from sqlalchemy.ext.automap import automap_base
# from sqlalchemy import Column, Integer, String
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.ext.automap import automap_base
# from sqlalchemy import create_engine
# from sqlalchemy import MetaData, Table


'''CREATE ENGINE'''
engine=create_engine('sqlite:///db_test.db')


'''GET TABLES NAMES'''
tables = engine.table_names()


'''START SESSION'''
session = Session(engine)


'''INIT MetaData'''
metadata = MetaData()


'''CATCH TABLE AS TABLE VAR'''
#table_name = Table('table_nane_from_db', metadata, autoload=True, autoload_with=engine)
samer = Table('samer', metadata, autoload=True, autoload_with=engine)


'''GET DATA FROM DB'''
query = session.query(samer).filter(samer.columns.name.like('%la%'))
df_out = pd.read_sql(query.statement, engine)


'''INSERT From DateFrame To TABLE'''
user={'name':['Samer'],'lastname':['Salem']}
user = pd.DataFrame.from_dict(user)
user.to_sql(con=engine, name='samer', if_exists='append', index= False)       


df_out.to_sql(con=engine, name='samer', if_exists='append', index= False)
df_out.to_sql()

'''Get Table Info'''
print(repr(samer))


'''Fetch DATA in DataFrame'''
### USING LIKE:
query = session.query(samer).filter(samer.columns.name.like('%la%'))
df_out = pd.read_sql(query.statement, engine)


### USING EQUAL:
query = session.query(samer).filter(samer.columns.name == 'Samer')
df_out = pd.read_sql(query.statement, engine)


'''Fetch Data Using SQL Query'''
connection = engine.connect()
stmt = 'select * from samer where name = "Samer"'
result=connection.execute(stmt)
res=result.fetchall()
for i in res:
    print(i)



ins = samer.insert().values(name='One',lastname=240)
conn = engine.connect()
row_id = conn.execute(ins).lastrowid # will insert data and return the last row id! 

x = samer.select('select * where name =="One"')

ins = samer.update().where(samer.c.id==4).values(name='TWO',lastname=100)
conn = engine.connect()
row_id = conn.execute(ins).lastrowid # will insert data and return the last row id! 

for i in df_out.iloc:
    # print(i)
    for d in i:
        print(d)
        





