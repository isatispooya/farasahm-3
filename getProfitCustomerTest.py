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


def groupSymbol(group):
    group = group.assign(VolExpnd=group['Volume'].expanding(1).sum(), NetExpnd=group['NetPrice'].expanding(1).sum(),BuyExpnd=group['BuyNetPrice'].expanding(1).sum(),SelExpnd=group['SelNetPrice'].expanding(1).sum())
    group = group[group['VolExpnd'] == 0]


    return group

for c in codelist:
    if c not in codelistEnd:

        df = pd.DataFrame(cl.find({'TradeCode': c}, {'_id': 0, 'NetPrice': 1, 'Price': 1, 'TotalCommission': 1, 'TradeDate': 1, 'TradeSymbol': 1, 'TradeType': 1, 'Volume': 1, 'TradeSymbolAbs': 1}))
        df['TradeDate'] = pd.to_datetime(df['TradeDate'], format="%Y-%m-%dT%H:%M:%S")
        df = df.sort_values(by=['TradeDate'])
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
        df = df.dropna(subset=['Volume'])
        df['Volume'] = df['Volume'] * df['TradeType'].apply(lambda x: 1 if x == 'Buy' else -1)
        df['NetPrice'] = df['NetPrice'] * df['TradeType'].apply(lambda x: -1 if x == 'Buy' else 1)
        df['BuyNetPrice'] = df['NetPrice'] * df['TradeType'].apply(lambda x: 1 if x == 'Buy' else 0)
        df['SelNetPrice'] = df['NetPrice'] * df['TradeType'].apply(lambda x: 0 if x == 'Buy' else 1)
        df = df.groupby(by=['TradeSymbolAbs']).apply(groupSymbol)
        if not df.empty:
            Profit = df['NetExpnd'].sum()
            Buy = df['BuyExpnd'].sum()*-1
            Sel = df['SelExpnd'].sum()
            Rate = ((Sel / Buy) - 1) * 100
            Count = len(df)
            ListSymbols = list(set(df['TradeSymbolAbs']))
            dic = {'code':c,'Profit':Profit,'Buy':Buy,'Sel':Sel,'Rate':Rate,'Count':Count,'ListSymbols':ListSymbols}
        else:
            dic = {'code':c,'Profit':0,'Buy':0,'Sel':0,'Rate':0,'Count':0,'ListSymbols':[]}
        count +=1
        prossec = (len(codelistEnd) + count) / len(codelist)
        prossec = int(prossec*100)
        print(prossec, len(codelistEnd) + count, len(codelist),int(Rate*100)/100)
        dbCa['NetPrice'].insert_one(dic)
        time.sleep(0)

