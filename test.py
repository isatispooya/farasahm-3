import pymongo
import pandas as pd
import time

client = pymongo.MongoClient()
db = client['farasahm2']
cl = db['TradeListBroker']
dbCa = client['CloseAi']

codelist = cl.distinct('TradeCode')
codelistEnd = dbCa['NetPrice'].distinct('code') 
count = 0
def gro(group):
    group['value'] = group['Volume'] * group['Price']
    group['NetPrice'] = group['NetPrice'].sum()
    group['Price'] = group['value'].sum() / group['Volume'].sum()
    group['TotalCommission'] = group['TotalCommission'].sum()
    group['Volume'] = group['Volume'].sum()
    group = group[group.index==group.index.min()]
    return group

TradeCode = '61580179184'
df = pd.DataFrame(cl.find({"TradeCode":TradeCode},{'_id':0,'TradeDate':1,'TradeSymbol':1,'TradeType':1,'Volume':1,'dateInt':1,'TradeSymbolAbs':1,'Price':1,'TotalCommission':1,'NetPrice':1}))
df['TradeDate'] = pd.to_datetime(df['TradeDate'], format="%Y-%m-%dT%H:%M:%S")
df = df.sort_values(by=['TradeDate'])
df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
df = df.dropna(subset=['Volume'])
df = df.groupby(by=['dateInt','TradeSymbol','TradeType']).apply(gro)
print(df)
df.to_excel('ss.xlsx')
