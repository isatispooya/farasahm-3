import pymongo
import pandas as pd
import Fnc
import time
import datetime
import requests
from persiantools import characters, digits
import ApiMethods
from persiantools.jdatetime import JalaliDate
from Fnc import isFund , isOragh
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']


farasahmDb['TradeListBroker'].delete_many({"dateInt":14020628})
ApiMethods.GetAllTradeLastDate()