from Login import SendSms
from setting import farasahmDb
import pandas as pd


df = pd.DataFrame(farasahmDb['registerNoBours'].find({"symbol":"fevisa"},{'date':1,'نام و نام خانوادگی':1,'_id':0,'شماره تماس':1,'کد ملی':1}))
df = df[df['date']==df['date'].max()].to_dict('records')

for i in df:
    mobile = str(i['شماره تماس'])
    name = str(i['نام و نام خانوادگی'])
    nc = str(i['کد ملی'])
    text = f'سهامدار محترم '+name
    text = text +' با کد/شناسه ملی '+nc
    text = text + '\nبدین وسیله به اطلاع میرساند مجمع عمومی سالیانه شرکت صنایع مفتول ایساتیس پویا در تاریخ 10 خرداد 1403 روز پنجشنبه راس ساعت 10 صبح در محل کارخانه به آدرس یزد روبروی نیروگاه خورشیدی برگزار میشود لطفا راس ساعت حضور بهم رسانید'
    

    SendSms(mobile,text)
