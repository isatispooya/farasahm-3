import pandas as pd
import pymongo
from ApiMethods import GetCustomerMomentaryAssets
import datetime
import Fnc
import time
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']



farasahmDb['sandoq'].update_many({'symbol':'خاتم'},{"$set":{"type":"sabet"}})
