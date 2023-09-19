import pymongo
import pandas as pd
import Fnc
import time
import datetime
import requests
from persiantools import characters, digits
import ApiMethods
from persiantools.jdatetime import JalaliDate
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']



ApiMethods.get_asset_customer()




