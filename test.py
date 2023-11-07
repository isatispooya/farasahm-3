import pandas as pd
import pymongo
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']


d = farasahmDb['fixIncomeHistori'].distinct('name')
for i in d:
    date_nav = farasahmDb['fixIncomeHistori'].find({'name':i},{'dateInt':1,'nav':1,'_id':0})
    for j in date_nav:
        nav = j['nav']
        date = j['dateInt']
        if nav>0:
            farasahmDb['sandoq'].update_one({'symbol':i,'dateInt':int(date)},{'$set':{'nav':nav}})
            print(i, date)
