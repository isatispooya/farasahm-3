from pymongo import MongoClient
import pandas as pd
# اتصال به دیتابیس MongoDB
client = MongoClient('mongodb://localhost:27017/')
database = client['farasahm2']
collection = database['TradeListBroker']

date = collection.distinct('dateInt')
for i in date:
    df = pd.DataFrame(collection.find({'dateInt':i},{'_id':0}))
    b = len(df)
    try:
        df = df.drop_duplicates(subset=['MarketInstrumentISIN','NetPrice','TradeCode','TradeDate','TradeNumber'])
    except:
        pass
    a = len(df)
    if b>a:
        df = df.to_dict('records')
        print(i , b-a)
        collection.delete_many({'dateInt':i})
        collection.insert_many(df)
