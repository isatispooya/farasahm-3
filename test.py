import pandas as pd
import pymongo
from ApiMethods import GetCustomerMomentaryAssets, get_asset_customer
import datetime
import Fnc
import time
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']



get_asset_customer()
