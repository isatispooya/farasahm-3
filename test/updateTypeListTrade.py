import pandas as pd
import pymongo
from persiantools.jdatetime import JalaliDate

client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

def isFund(x):
    if 'صندوق' in x:
        return True
    else:
        return False

def isOragh(name):
    lst = ['صكوك','صکوک','اجاره','اجاره','مرابحه','مرابحه','گام','گام','گواهي اعتبار مولد','گواهی اعتبار مولد','تامين مالي','تامین مالی','اسنادخزانه','اسنادخزانه''خريد دين','خرید دین','منفعت','منفعت','مشاركت','مشارکت','امتيازتسهيلات مسكن','امتیازتسهیلات مسکن']
    for l in lst:
        if (l in name):
            return True
    else:
        return False

df = farasahmDb['tse'].find({},{'نماد':1,'نام':1,'_id':0})
df = pd.DataFrame(df)
df = df.drop_duplicates()
df = df.reset_index()
for i in df.index:
    name = df['نام'][i]
    oragh = isOragh(name)
    if oragh:
        farasahmDb['tse'].update_many({'نام':name},{'$set':{'InstrumentCategory':True}})
        print(i, name)

