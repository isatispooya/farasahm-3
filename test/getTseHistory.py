import requests
import pandas as pd
from persiantools.jdatetime import JalaliDate
import datetime
from persiantools import characters, digits
import pymongo
client = pymongo.MongoClient()
db = client['farasahm2']


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

today = datetime.datetime.now()
newDate = today
for i in range(0,365,1):
    newDate = today - datetime.timedelta(days=i)
    dt = datetime.datetime(newDate.year,newDate.month,newDate.day,15,0,0)
    jalali = JalaliDate.to_jalali(newDate)
    jalaliStr = str(jalali).replace('-','/')
    jalaliInt =int(str(jalali).replace('-',''))
    chack = db['tse'] .count_documents({'dataInt':jalaliInt})
    if chack == 0:
        res = requests.get(url=f'http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d={jalali}')
        if res.status_code == 200:
            df = pd.read_excel(res.content,header=2, engine='openpyxl')
            if len(df)>10:
                df['نماد'] = df['نماد'].apply(characters.ar_to_fa)
                df['نام'] = df['نام'].apply(characters.ar_to_fa)
                df['صندوق'] = df['نام'].apply(isFund)
                df['InstrumentCategory'] = df['نام'].apply(isOragh)
                df['data'] = jalaliStr
                df['dataInt'] = jalaliInt
                df['timestump'] = dt.timestamp()
                df['time'] = str(dt.hour) +':'+str(dt.minute)+':'+str(dt.second)
                df = df.to_dict('records')
                db['tse'].delete_many({'dataInt':jalaliInt})
                db['tse'].insert_many(df)



                

                


                
