from pymongo import MongoClient
import Fnc
import pandas as pd
import datetime
# اتصال به دیتابیس MongoDB
client = MongoClient()
db = client['farasahm2']
import ApiMethods





man = ApiMethods.GetCustomerRemainWithTradeCode('61593430334772','TSE')
listCode = db['TradeListBroker'].distinct('TradeCode')
count = 0
for i in listCode:
    if count>=5000:
        break
    print(i)
    try:
        dic = ApiMethods.GetCustomerRemainWithTradeCode(i,'TSE')
        dic['TradeCode'] = i
        dic['Datetime'] = datetime.datetime.now()
    except:
        pass
    print(dic)
    db['CustomerRemain'].insert_one(dic)
    count = count + 1