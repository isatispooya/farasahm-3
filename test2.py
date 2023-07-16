import pandas as pd
import pymongo
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']


df =pd.read_excel(r"C:\Users\isatis pouya\Desktop\New folder\Book2.xlsx",dtype=str)
df = df.fillna('')
df['تعداد سهام'] = [int(x) for x in df['تعداد سهام']]
df['symbol'] = 'devisa'
df['date'] = 14020420
df['rate'] = df['تعداد سهام'] / df['تعداد سهام'].sum()
df['rate'] = [int(x*10000)/100 for x in df['rate']]
df = df.to_dict('records')
farasahmDb['registerNoBours'].delete_many({'symbol':'devisa'})
farasahmDb['registerNoBours'].insert_many(df)
