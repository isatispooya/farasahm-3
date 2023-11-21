import pandas as pd
import pymongo
from ApiMethods import get_asset_funds

client = pymongo.MongoClient()
farasahm_db = client['farasahm2']

farasahm_db['sandoq'].update_many({},{'$set':{'navAmary':0}})
