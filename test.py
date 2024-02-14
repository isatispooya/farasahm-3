from pymongo import MongoClient
import Fnc
import pandas as pd
import datetime
import dataManagment
# اتصال به دیتابیس MongoDB
client = MongoClient()
db = client['farasahm2']



for i in range(14021101,14021131):
    df = pd.DataFrame(db['TradeListBroker'].find({'dateInt':i},{'_id':0})).drop_duplicates(subset=['NetPrice','TradeCode','TradeDate','TradeNumber','TradeSymbol'])
    db['TradeListBroker'].delete_many({'dateInt':i})
    db['TradeListBroker'].insert_many(df.to_dict('records'))
