import pymongo
import pandas as pd
import time
import ApiMethods

client = pymongo.MongoClient()
db = client['farasahm2']

d = ApiMethods.GetCustomerByNationalCode('4420278862')
print(d)


