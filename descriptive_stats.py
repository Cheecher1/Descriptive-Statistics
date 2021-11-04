#Script to connect to snowflake and run descriptive analysis of continuous and categorical variables

# !pip install snowflake_connector_python
#!pip install snowflake-connector-python[pandas]
#!pip install holidays
#!pip install --upgrade snowflake-sqlalchemy
import snowflake.connector
import pandas as pd
import datetime as dt
import boto3
import base64
from numpy import dtype, isnan, sqrt
from sqlalchemy import create_engine
from sqlalchemy import pool
from sqlalchemy.dialects import registry
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from botocore.exceptions import ClientError
from snowflake.connector.pandas_tools import write_pandas, pd_writer
from dateutil.easter import *

###   Connecting to the database   ####

ctx = snowflake.connector.connect(
      user='<username>',
      account='<account>',
      password = '<password>',
      warehouse='<warehouse>',
      database='<client>',
      schema='<schema>'
  )
cur = ctx.cursor()
sql = """
          SELECT * FROM ,client.scheme.table.
          SAMPLE (<desired sample size>)
          WHERE VIDEODATE BETWEEN '<INTIAL DATE>' AND '<END DATE>' 
  """
cur.execute(sql)
# Get the result set from the cursor and deliver it as the Pandas DataFrame.
df = cur.fetch_pandas_all()

cur.close()
ctx.close()

#Display first few rows of pandas data
df.head()

#Descriptive Statistics for Continuous Variables including Counts of Nulls and Zeros and histogram

def cont_desc(column_name):
    min = df[column_name].min()
    median = df[column_name].median()
    max = df[column_name].max()
    mean = df[column_name].mean()
    mode = df[column_name].mode()
    stdev = df[column_name].std()
    skew = df[column_name].skew()
    kurtosis = df[column_name].kurtosis()
    nulls = df[column_name].isna().sum()
    zeros = (df[column_name] == 0).sum()

    desc_data = [['Min', min], ['Median', median], ['Max', max],['Mean', mean],['Mode', mode],['Stdev', stdev],['Skew', skew],['Kurtosis', kurtosis], ['Nulls',nulls] ]

    descriptives = pd.DataFrame(desc_data, columns = ['Descriptive', 'Value'])
    hist = df.hist(column = column_name, bins = 30)
    return(descriptives)

#Descriptive Statistics for Categorical Variables including Counts of Nulls and Zeros and bar chart for categories < 15

def cat_desc(column_name):
    counts = df[column_name].value_counts(ascending=False).rename_axis('Values').reset_index(name='Counts')
    
    if df[column_name].nunique() < 15:
        ax = counts.plot.barh(x='Values', y='Counts', rot=0)
    else: print('Too Many Categories to Plot!')
   
    mode = df[column_name].mode()
    nulls = df[column_name].isna().sum()
    zeros = (df[column_name] == 0).sum()

    desc_data = [['Min', min], ['Median', median], ['Max', max],['Mean', mean],['Mode', mode]]

    descriptives = pd.DataFrame(desc_data, columns = ['Descriptive', 'Value'])

    
    return(descriptives, counts)

