import pandas as pd
import pymongo
from ApiMethods import GetCustomerMomentaryAssets, get_asset_customer
import datetime
import Fnc
import time
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

farasahmDb['PriorityPay'].update_many({},{'$set':{'capDate':"1402/04/21"}})
farasahmDb['PriorityTransaction'].update_many({},{'$set':{'capDate':"1402/04/21"}})

