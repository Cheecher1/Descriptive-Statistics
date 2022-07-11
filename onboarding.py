import snowflake
import snowflake.connector
import pandas as pd

con = snowflake.connector.connect(
    user='DANIEL',
    password='@Edline294425',
    account='mja29153.us-east-1',
    warehouse='WORKSHEET_XS',
    database='CLIENT',
    schema='HYUNDAI'
)

cur = con.cursor()
cur.execute("select * from CLIENT.HYUNDAI.REDS_IMPRESSIONS LIMIT 1000")
df = cur.fetch_pandas_all()
cur.close()
df

import matplotlib as lib
import webbrowser
import matplotlib.pyplot as plt
col = 'ADVERTISERCOSTINUSDOLLARS'
groupby ='ZIP'
lables = ["Mean", "meadium"]
showHistorgram = True
def stats(df, col, groupby, showHistorgram= False):
    
         #stats
         mean = df.groupby(groupby)[col].mean()
         medium = df.groupby(groupby)[col].median()
         standarddev = df.groupby(groupby)[col].std()
         skew = df.groupby(groupby)[col].skew()
         kurt = df.kurtosis()
         print(mean, medium, standarddev, skew, kurt)
         freq = mean.value_counts()
         print(freq)
         
         
         
         #print histogram
         if (showHistorgram):
             hist = mean.hist(bins = 30)
             plt.title("Most frequent mean adcost by ZIP ")
             plt.xlabel("ADCOSTINDOLLAR Mean")
             plt.ylabel("FREQ")
             plt.savefig("price_by_zipcode")
             print(hist)
        
         
         #write to html file 
         file = open("Stats description.html", "w")
         newmean = mean.to_frame().to_html()
         file.write(newmean + '&nbsp;')
         file.write(newmean + '&nbsp;')
         file.close()
         webbrowser.open("Stats description.html")
         
        
         
    
stats(df, col, groupby, showHistorgram)
