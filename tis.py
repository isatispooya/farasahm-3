import pandas as pd
from setting import farasahmDb


df = pd.DataFrame(farasahmDb['Priority'].find({'symbol':'fevisa','dateInt':14030728},{'شماره تماس':1,'کد ملی':1}))
df = df.dropna(subset=['شماره تماس'])
df['شماره تماس'] = df['شماره تماس'].apply(lambda x: str(x).split(".")[0])
df['شماره تماس'] = df['شماره تماس'].apply(lambda x: '0'+str(int(x))[-10:])
df.to_excel('fevisa.xlsx',index=False)
for i in df['شماره تماس'].to_list():

    print(i)

