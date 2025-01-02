import pandas as pd
import pymongo
from Login import SendSms

digital_market_db = pymongo.MongoClient('mongodb://localhost:27017/')['digital_market_db']

df = pd.DataFrame(digital_market_db.tamadon.find({'clean':True,'isatispooya':False}))
