from setting import farasahmDb
import pandas as pd

df = pd.DataFrame(farasahmDb['customerofbroker'].find())
farasahmDb['customerofbroker'].delete_many({})
print(df)
df = df.drop_duplicates(subset='PAMCode').drop(columns=['_id'])
df = df.to_dict('records')
farasahmDb['customerofbroker'].insert_many(df)
print(df)