from pymongo import MongoClient
import Fnc
import pandas as pd
import datetime
# اتصال به دیتابیس MongoDB
client = MongoClient()
db = client['farasahm2']
import ApiMethods


