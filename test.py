import pandas as pd
import pymongo
from ApiMethods import GetCustomerMomentaryAssets
import datetime
import Fnc
import time
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']



TradeCodes = farasahmDb['TradeListBroker'].distinct('TradeCode')
today = datetime.datetime.now()
dateInt = Fnc.gorgianIntToJalaliInt(today)
for i in range(0,len(TradeCodes)):
    TradeCode = TradeCodes[i]
    print(TradeCode, '-',processe)
    inDb = farasahmDb['assetCoustomerOwnerFix'].find_one({'TradeCode':TradeCode})
    if inDb == None:
        processe = round((((i+1) / len(TradeCodes))*100),2)
        asset = pd.DataFrame(GetCustomerMomentaryAssets(TradeCode))
        if len(asset)>0:
            asset['datetime'] = today
            asset['dateInt'] = dateInt
            asset = asset.to_dict('records')
            farasahmDb['assetCoustomerOwnerFix'].delete_many({'TradeCode':TradeCode, 'dateInt':dateInt})
            farasahmDb['assetCoustomerOwnerFix'].insert_many(asset)
            time.sleep(1)