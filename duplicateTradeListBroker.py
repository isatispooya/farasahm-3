from setting import farasahmDb, pishkarDb
import pandas as pd
from Login import SendSms
# import random
# def ch(num):
#     num = str(num)
#     if len(num) < 10:
#         return False
#     try:
#         num = int(num)
#     except:
#         return False
#     num = '0'+str(num)
#     if len(num) != 11:
#         return False
#     return num
# df_reg = pd.DataFrame(farasahmDb['registerNoBours'].find({},{'شماره تماس':1}))
# df_bor = pd.DataFrame(farasahmDb['customerofbroker'].find({},{'Mobile':1}))
# df_ins = pd.DataFrame(pishkarDb['customers'].find({},{'تلفن همراه':1}))

# df_reg = df_reg.rename(columns={'شماره تماس':'mobile'})
# df_bor = df_bor.rename(columns={'Mobile':'mobile'})
# df_ins = df_ins.rename(columns={'تلفن همراه':'mobile'})

# df = pd.concat([df_reg,df_bor,df_ins])
# df = df.dropna()
# df['mobile'] = df['mobile'].apply(ch)
# df = df[df['mobile']!=False]
# df = df.drop_duplicates(subset=['mobile'])
# df.to_excel('df.xlsx')

import pandas as pd
from concurrent.futures import ThreadPoolExecutor

txt = 'مشتری گرامی ایساتیس پویا\nفرصت ویژه سرمایه‌گذاری با سود سالانه 43 درصد در سکوی تأمین مالی ایساتیس کراد\nisatiscrowd.ir'

def send_sms_to_mobile(mobile):
    SendSms(mobile, txt)

df = pd.read_excel('d351.xlsx', dtype=str)
# df['sss'] = ['isatiscrowd.ir' in x for x in df['پیش شماره']]
# df['mobile'] = ['0' + str(x) for x in df['گیرنده']]
# df =df[df['sss']==True]
# print(df)
# dff = pd.read_excel('df.xlsx', dtype=str)
# dff['kk'] = [x not in df['mobile'].to_list()  for x in dff['mobile']]
# dff = dff[dff['kk'] == True]
# dff.to_excel('df.xlsx')
with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(send_sms_to_mobile, df['mobile'])
    



# d351 = []
# for i in range(0,9999):
#     print(i)
#     n = str(i)
#     if len(n)<4:
#         ziro = (4 - len(n))*'0'
#         n = ziro + n
#     num = '0913151'+n
#     if num not in df:
#         d351.append(num)
#     num = '0913152'+n
#     if num not in df:
#         d351.append(num)
#     num = '0913153'+n
#     if num not in df:
#         d351.append(num)
#     num = '0913154'+n
#     if num not in df:
#         d351.append(num)


# df_351 = pd.DataFrame(d351, columns=['mobile']) 
# df_351.to_excel('d351.xlsx', index=False)

# print(len(d351))





# print(len(d351))