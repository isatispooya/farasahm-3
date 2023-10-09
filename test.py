import pymongo
import pandas as pd
import time
import ApiMethods

client = pymongo.MongoClient()
db = client['farasahm2']

protfo = ApiMethods.GetCustomerMomentaryAssets('61580209324')
asset = ApiMethods.GetCustomerRemain('61580209324','TSE')


print(protfo)

