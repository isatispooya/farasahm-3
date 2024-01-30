from pymongo import MongoClient
import Fnc
import pandas as pd
# اتصال به دیتابیس MongoDB
client = MongoClient()
db = client['farasahm2']
import ApiMethods





man = ApiMethods.GetCustomerRemainWithTradeCode('61593430334772','TSE')
print(man)