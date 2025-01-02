import pandas as pd
from setting import farasahmDb
from Login import SendSms


df = pd.read_excel('fevisa.xlsx')
df = df[df['شماره تماس']!=0]
df = df[df['حق تقدم']>0]
df['حق تقدم'] = df['حق تقدم'].astype(str)
df['شماره تماس'] = df['شماره تماس'].astype(str)
df['شماره تماس'] = df['شماره تماس'].apply(lambda x: '0' +x[-10:])
df['text'] = 'سهامدار محترم صنایع مفتول ایساتیس پویا\n'+'مهلت استفاده از حق تقدم تا تاریخ 1403/10/10 تمدید گردید\n' +'تعداد حق تقدم:'+ df['حق تقدم'] +'\n'  +'isatispooya.com'





for i in df.index:
    SendSms(df['شماره تماس'][i],df['text'][i])



