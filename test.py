from pymongo import MongoClient
import Fnc
import ApiMethods
# اتصال به دیتابیس MongoDB
client = MongoClient('mongodb://localhost:27017/')
database = client['farasahm2']
collection = database['TradeListBroker']

date = collection.distinct('dateInt')
for i in date:
    collection.delete_many({'dateInt':i})
    if i<14020327:
        toDay = str(i)
        toDay = [toDay[:4],toDay[4:6],toDay[6:8]]
        toDay = [int(x) for x in toDay]
        toDayIntJal = i
        ApiMethods.GetAllTradeInDate(toDay,toDayIntJal)

