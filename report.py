
from flask import send_file
from sympy import symbols, Eq, solve

import json
import pandas as pd
from io import StringIO
import pymongo
from persiantools.jdatetime import JalaliDate
import jdatetime
import datetime
from dataManagment import lastupdate
import numpy as np
client = pymongo.MongoClient()
from persiantools.jdatetime import JalaliDate
from PIL import Image, ImageDraw, ImageFont
import arabic_reshaper
from bidi.algorithm import get_display
from persiantools import characters, digits

from bson import ObjectId
import Fnc
from ApiMethods import GetCustomerMomentaryAssets
import time
from bson import Binary
from moadian import Moadian
from bson.son import SON

from reportlab.lib.pagesizes import letter
from reportlab.lib.pagesizes import landscape, A4
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from setting import farasahmDb

symbolTarget = ['نهال1', 'زرفام1', 'نفیس1', 'افران1', 'سپر1', 'کیان1', 'کمند1', 'پارند1', 'تصمیم1', 'دیبا1', 'تمشک1', 'امین یکم1', 'ارزش1', 'همای1', 'سرو1', 'هامرز1', 'آرام1', 'مانی1', 'آکورد1', 'کارا1', 'آگاس1', 'ثبات1', 'سپیدما1', 'توان1', 'گام0208151', 'اطلس1', 'هم وزن1', 'اعتماد1', 'کامیاب1', 'پاداش1', 'آوند1', 'ثنا1', 'کاج1', 'الماس1', 'لبخند1', 'آساس1', 'رماس1', 'نخل1', 'کاریس1', 'داریک1', 'کارین1', 'فیروزا1', 'انار1', 'فردا1', 'کهربا1', 'کاردان1', 'سلام1', 'آفاق1', 'دریا1', 'زیتون1', 'اراد501', 'گنج1', 'ویستا1', 'آسامید1', 'سام1', 'یاقوت1', 'ثهام1', 'آوا1', 'آتیمس1', 'داریوش1', 'تدبیر041', 'رشد1', 'فراز1', 'ثمین1', 'فیروزه1', 'وصندوق1', 'اوصتا1', 'اعتبار1', 'ساحل1', 'هیوا1', 'برلیان1', 'اکسیژن1', 'زرین1', 'دارا1', 'آسام1', 'رابین1', 'یارا1', 'آلتون1', 'درسا1', 'وبازار1', 'اخزا1031', 'اخزا1032', 'اخزا0022', 'اخزا0021', 'درین1', 'گام0208132', 'گام0208131', 'گنجینه1', 'پادا1', 'صنم', 'مروارید1', 'صنوین1', 'بذر1', 'صایند1', 'اوج1', 'اخزا0031', 'اخزا0032', 'گام0207132', 'افق ملت1', 'اگ0201551', 'پرتو1', 'اخزا9101', 'سپاس1', 'اخزا9091', 'اخزا1011', 'ثروتم1', 'اخزا0051', 'اخزا0071', 'عقیق1', 'سخند1', 'اخزا9081', 'گنجین1', 'تاراز1', 'اخزا0091', 'اراد502', 'اخزا9141', 'اخزا1061', 'اخزا1071', 'اخزا1012', 'اخزا0101', 'اخزا0102', 'پتروما1', 'مهریران1', 'ناب1', 'اخزا9102', 'رایکا1', 'اراد871', 'اخزا8211', 'اخزا0042', 'اخزا0072', 'اخزا0041', 'اخزا1041', 'اخزا0052', 'اراد991', 'اخزا0012', 'اخزا0011', 'اخزا8201', 'نیلی1', 'گام0207561', 'اخزا1042', 'گام0208152', 'شتاب1', 'آکام1', 'اخزا0061', 'اخزا0062', 'پایا1', 'گام0206132', 'جهش1', 'اخزا9142', 'اراد1121', 'اخزا1051', 'صنفت13121', 'اخزا0092', 'اخزا9082', 'اخزا1081', 'آلا1', 'صبا14041', 'اخزا1082', 'سیناد1', 'استیل1', 'فاخر1', 'اخزا1044', 'صپترو7051', 'اگ0205551', 'صنهال1', 'بازده1', 'پتروداریوش1', 'نشان1', 'طلوع1', 'صدف1', 'سمان1', 'پتروصبا1', 'هوشیار1', 'بهین رو1', 'ترمه2', 'توسکا1', 'اراد1371', 'تیام1', 'فلزفارابی1', 'اراد1071', 'آذرین1', 'متال1', 'کرمان4621', 'اخزا1072', 'اخزا1062', 'اراد992', 'جواهر1', 'خورشید1', 'صاف فیلم521', 'ماهور1', 'تابش1', 'اتوآگاه1', 'خلیج1', 'پیروز1', 'اخزا2021', 'اخزا2022', 'آفرین1', 'صترا5092']

def Day_list():  
    yaer = ['1399','1400','1401','1402']
    mon = ['01','02','03','04','05','06','07','08','09','10','11','12']
    day = []
    listday= []
    for x in range(32):
        if x != 0:
            if len(str(x))==1:
                day.append('0'+str(x))
            else:
                day.append(str(x))
    for y in yaer:
        for m in mon:
            if int(m)<7:
                for d in day:
                    listday.append(y+m+d)
            if int(m)>=7 and int(m) != 12:
                for d in day[:-1]:
                    listday.append(y+m+d)
            if int(m) == 12 and y!='1399':
                for d in day[:-2]:
                    listday.append(y+m+d)
            if int(m) == 12 and y=='1399':
                for d in day[:-1]:
                    listday.append(y+m+d)
    listday = [int(x) for x in listday]
    return listday

def periodListGenerate():
    toDayGorgian =datetime.datetime.now()
    periodList = []
    for i in [1,2,3,6,12]:
        period = toDayGorgian - datetime.timedelta(days=i*30)
        period = JalaliDate(period)
        period = int(str(period).replace('-',''))
        periodList.append({'period':i,'date':period})
    return periodList

def dateIntToDateGorgiaStr(num):
    st = str(num)
    y = st[:4]
    m = st[4:6]
    d = st[6:8]
    stgorgia = JalaliDate(int(y), int(m), int(d)).to_gregorian()
    m = str(stgorgia.month)
    d = str(stgorgia.day)
    if len(m) == 1 :m = '0' + m
    if len(d) == 1 :d = '0' + d
    return str(stgorgia.year)+'-'+m+'-'+d

def gettoptraders(data):
    date = datetime.datetime.fromtimestamp(data['date']/1000)
    date = int(str(JalaliDate.to_jalali(date.year, date.month, date.day)).replace('-',''))
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['traders'].find({'symbol':symbol,'date':date},{'fullname':1,'_id':0,'تعداد خرید':1,'تعداد فروش':1}))
    if len(df) == 0:
        return json.dumps({'replay':False,'msg':'داده ای موجود نیست'})
    dfBuy = df.sort_values(by=['تعداد خرید'],ascending=False).reset_index()
    dfSel = df.sort_values(by=['تعداد فروش'],ascending=False).reset_index()
    dfBuy = dfBuy[dfBuy['تعداد خرید']>0][['fullname','تعداد خرید']]
    dfSel = dfSel[dfSel['تعداد فروش']>0][['fullname','تعداد فروش']]
    dfBuy.columns = ['name','vol']
    dfSel.columns = ['name','vol']
    dfBuy = dfBuy[dfBuy.index<10]
    dfBuy.index = dfBuy['name']
    dfSel = dfSel[dfSel.index<10]
    dfSel.index = dfSel['name']
    dfBuy = dfBuy.to_dict(orient='dict')
    dfSel = dfSel.to_dict(orient='dict')
    return json.dumps({'replay':True,'df':{'buy':dfBuy,'sel':dfSel}})

def gettopbroker(data):
    date = datetime.datetime.fromtimestamp(data['date']/1000)
    date = int(str(JalaliDate.to_jalali(date.year, date.month, date.day)).replace('-',''))
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['broker'].find({'symbol':symbol,'date':date},{'broker':1,'_id':0,'تعداد خرید':1,'تعداد فروش':1}))
    if len(df) == 0:
        return json.dumps({'replay':False,'msg':'داده ای موجود نیست'})
    dfBuy = df.sort_values(by=['تعداد خرید'],ascending=False).reset_index()
    dfSel = df.sort_values(by=['تعداد فروش'],ascending=False).reset_index()
    dfBuy = dfBuy[dfBuy['تعداد خرید']>0][['broker','تعداد خرید']]
    dfSel = dfSel[dfSel['تعداد فروش']>0][['broker','تعداد فروش']]
    dfBuy.columns = ['name','vol']
    dfSel.columns = ['name','vol']
    dfBuy = dfBuy[dfBuy.index<10]
    dfBuy.index = dfBuy['name']
    dfSel = dfSel[dfSel.index<10]
    dfSel.index = dfSel['name']
    dfBuy = dfBuy.to_dict(orient='dict')
    dfSel = dfSel.to_dict(orient='dict')
    return json.dumps({'replay':True,'df':{'buy':dfBuy,'sel':dfSel}})


def getnewcomer(data):
    date = datetime.datetime.fromtimestamp(data['date']/1000)
    date = int(str(JalaliDate.to_jalali(date.year, date.month, date.day)).replace('-',''))
    symbol = data['access'][1]
    dic = farasahmDb['newComer'].find_one({'symbol':symbol,'date':date},{'_id':0})
    if dic == None: return ({'replay':False})
    return json.dumps({'replay':True,'dic':dic})


def getdftraders(data):
    date = datetime.datetime.fromtimestamp(data['date']/1000)
    date = int(str(JalaliDate.to_jalali(date.year, date.month, date.day)).replace('-',''))
    symbol = data['access'][1]
    df = pd.DataFrame((farasahmDb['traders'].find({'symbol':symbol,'date':date},{'_id':0})))
    if len(df)==0:
        return json.dumps({'replay':False})
    dic = {'VolSum':df['تعداد خرید'].sum(),'VolMax':df['تعداد خرید'].max()}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})

def getnewcomerall(data):
    symbol = data['access'][1]
    df = pd.DataFrame((farasahmDb['newComer'].find({'symbol':symbol},{'_id':0}))).sort_values('date',ascending=False)
    if len(df)==0:
        return json.dumps({'replay':False})
    df['ratNewVol'] = [int(x*100) for x in df['ratNewVol']]
    df['ratNewCnt'] = [int(x*100) for x in df['ratNewCnt']]
    df['ratOutVol'] = [int(x*100) for x in df['ratOutVol']]
    df['ratOutCnt'] = [int(x*100) for x in df['ratOutCnt']]
    df = df.rename(columns={"newcomer":"_children"})

    dff = pd.DataFrame(farasahmDb['features'].find({'symbol':symbol},{'_id':0,'date':1,'predict_CountNewBuyer':1}))
    dff = dff.set_index('date').fillna(0)
    dff['predict_CountNewBuyer'] = [int(x) for x in dff['predict_CountNewBuyer']]
    df = df.set_index('date').join(dff).reset_index()

    dic = {'allVol':int(df['allVol'].max()),'allCntBuy':int(df['allCntBuy'].max()),'outVol':int(df['outVol'].max()),
           'newVol':int(df['newVol'].max()),'allCntSel':int(df['allCntSel'].max()),"newCnt":int(df['newCnt'].max()),
           'ratNewVol':int(df['ratNewVol'].max()),'ratNewCnt':int(df['ratNewCnt'].max()),"outCnt":int(df['outCnt'].max()),
           'ratOutVol':int(df['ratOutVol'].max()),'ratOutCnt':int(df['ratOutCnt'].max())}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getstation(data):
    date = datetime.datetime.fromtimestamp(data['date']/1000)
    date = int(str(JalaliDate.to_jalali(date.year, date.month, date.day)).replace('-',''))
    symbol = data['access'][1]
    df = pd.DataFrame((farasahmDb['station'].find({'symbol':symbol,'date':date},{'_id':0,'symbol':0})))
    if len(df)==0:
        return json.dumps({'replay':False})
    dic = {'cntBuy':int(df['تعداد خرید'].max()),'cntSel':int(df['تعداد فروش'].max())}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getstockman(data):
    date = datetime.datetime.fromtimestamp(data['date']/1000)
    date = int(str(JalaliDate.to_jalali(date.year, date.month, date.day)).replace('-',''))
    symbol = data['access'][1]
    df = pd.DataFrame((farasahmDb['register'].find({'symbol':symbol,'تاریخ گزارش':date},{'_id':0,'سهام کل':1,'کد سهامداری':1,'fullName':1,'تاریخ تولد':1,'محل صدور':1,'کد ملی':1,'اخرین کارگزاری خرید':1,'اخرین کارگزاری فروش':1,'نام پدر':1})))
    if len(df)==0:
        return json.dumps({'replay':False})
    df['سهام کل'] =[int(x) for x in df['سهام کل']]
    df['rate'] =[int((x/df['سهام کل'].sum())*10000)/100 for x in df['سهام کل']]
    dic = {'all':int(df['سهام کل'].max())}
    df = df.fillna('')
    df = df.sort_values(by='سهام کل',ascending=False).to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getallname(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['traders'].find({'symbol':symbol,},{'کد':1,'_id':0,'fullname':1}))
    if len(df) == 0 :return json.dumps({'replay':False})
    df = df.drop_duplicates()
    df.columns = ['code', 'fullname']
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})

def getallnameNoBours(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol,},{'_id':0,'نام و نام خانوادگی':1}))
    if len(df) == 0 :return json.dumps({'replay':False})
    df = df.drop_duplicates()
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})


def getdetailstrade(data):
    symbol = data['access'][1]
    trader = data['trader']
    dicFild = {'_id':0,'تاریخ معامله':1,'نام خانوادگی خریدار':1,'نام خریدار':1,'نام خانوادگی فروشنده':1,'نام فروشنده':1,'تعداد سهم':1,'قیمت هر سهم':1,'تاریخ معامله':1}
    dfb = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol,'کد خریدار':trader},dicFild))
    dfs = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol,'کد فروشنده':trader},dicFild))
    if len(dfs):
        dfs['تعداد سهم'] = dfs['تعداد سهم'] * -1
    df = pd.concat([dfb,dfs]).fillna('')
    df['fullnameBuy'] = df['نام خریدار'] +' ' +df['نام خانوادگی خریدار']
    df['fullnameSel'] = df['نام فروشنده'] +' ' +df['نام خانوادگی فروشنده']
    df['حجم'] = [abs(x) for x in df['تعداد سهم']]
    df = df.sort_values(by='تاریخ معامله',ascending=False)
    dic = {'maxVol':int(df['حجم'].max())}
    df = df.drop(columns=['حجم','نام خریدار','نام فروشنده','نام خانوادگی خریدار','نام خانوادگی فروشنده'])
    df = df.to_dict("records")
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getnav(data):
    etf = farasahmDb['menu'].find_one({'name':data['access'][1]})
    symbol = etf['symbol']
    history = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbol},{'_id':0,'date':1,'final_price':1,'nav':1,'trade_volume':1,'navAmary':1}))
    history = history.fillna(0)
    history['diff'] = history['nav'] - history['final_price']
    history['rate'] = ((history['nav'] / history['final_price'])-1)*100
    history['rate'] = [round(x,2) for x in history['rate']]
    history['diffAmary'] = history['navAmary'] - history['final_price']
    history['rateAmary'] = ((history['navAmary'] / history['final_price'])-1)*100
    history['rateAmary'] = [round(x,2) for x in history['rateAmary']]
    history['trade_volume'] = [int(x) for x in history['trade_volume']]
    history['date'] = [int(str(x).replace('/','')) for x in history['date']]
    history = history.sort_values(by=['date'],ascending=False).reset_index()
    history = history.drop(columns='index')
    dic = {'diff':float(history['diff'].max()), 'rate':float(history['rate'].max()),'diffAmary':float(history['diffAmary'].max()), 'rateAmary':float(history['rateAmary'].max()),'volume':float(history['trade_volume'].max())}
    history = history.to_dict('records')
    return json.dumps({'replay':True,'df':history,'dic':dic})



def getreturn(data):
    etf = farasahmDb['menu'].find_one({'name':data['access'][1]})
    name = etf['symbol']
    target = data['input']['target']
    df = pd.DataFrame(farasahmDb['sandoq'].find({'type':'sabet'},{'_id':0}))
    df = df[df['symbol']==name]
    df = df.groupby(by='symbol').apply(Fnc.fund_compare_clu_ccp)
    df = df.reset_index()
    if 'level_1' in df.columns:
        df = df.drop(columns=['level_1'])
    df = df.to_dict('records')[0]
    lst = [
        {'indx':'7','ret':df['ret_period_7'],'ytm':df['ret_ytm_7'],'smp':df['ret_smp_7']},
        {'indx':'14','ret':df['ret_period_14'],'ytm':df['ret_ytm_14'],'smp':df['ret_smp_14']},
        {'indx':'30','ret':df['ret_period_30'],'ytm':df['ret_ytm_30'],'smp':df['ret_smp_30']},
        {'indx':'90','ret':df['ret_period_90'],'ytm':df['ret_ytm_90'],'smp':df['ret_smp_90']},
        {'indx':'180','ret':df['ret_period_180'],'ytm':df['ret_ytm_180'],'smp':df['ret_smp_180']},
        {'indx':'365','ret':df['ret_period_365'],'ytm':df['ret_ytm_365'],'smp':df['ret_smp_365']},
        {'indx':'730','ret':df['ret_period_730'],'ytm':df['ret_ytm_730'],'smp':df['ret_smp_730']},
    ]

    df = pd.DataFrame(lst)
    df['dif'] = df['ytm'] - target
    dic = {}
    for i in ['ytm','smp','dif']:
        dic[i] = df[i].apply(abs).max()
    df = df.to_dict(orient='records')
    return json.dumps({'replay':True,'df':df,'dic':dic})

def getcompare(data):
    etfs = pd.DataFrame(farasahmDb['sandoq'].find({'type':'sabet'},{'_id':0}))
    ts = etfs[etfs['symbol']=='خاتم']
    ts = ts.sort_values(by=['dateInt'],ascending=False)

    etfs = etfs.groupby(by='symbol').apply(Fnc.fund_compare_clu_ccp)
    etfs = etfs.reset_index().drop(columns=['level_1'])

    dic = {}
    for i in etfs.columns:
        if not i in ['symbol','update']:
            dic['i'] = etfs[i].max()
    etfs = etfs.dropna()
            
    df = etfs.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic}) 



#def getcompare(data):
#    periodList = [1,14,30,90,180,365,730]
#    etfSelect = [x['نماد'] for x in farasahmDb['fixIncome'].find({},{'_id':0,'نماد':1})]
#    dff = pd.DataFrame()
#    onDate = False
#    for i in etfSelect:
#            psPeriod = list(farasahmDb['fixIncome'].find({'نماد':i}))[0][' دوره تقسیم سود ']
#            if psPeriod=='ندارد':
#                df = pd.DataFrame(farasahmDb['fixIncomeHistori'].find({'name':i},{ 'dateInt':1, '_id':0,'final_price':1,'nav':1})).set_index('dateInt')
#                day_list = Day_list()
#                day_list =  filter(lambda x: x >= df.index.min(), day_list)
#                day_list =  filter(lambda x: x <= df.index.max(), day_list)
#                df = df.join(pd.DataFrame(index=day_list), how='right')
#                df = df.sort_index(ascending=True)
#                nav = df.iloc[-30:]
#                nav = nav[nav['nav']>0]
#                nav = nav[nav['nav']!=np.nan]
#                nav['diff'] = (nav['final_price'] / nav['nav'])-1
#                nav['diff'] = nav['diff'] * 100
#                navlast = nav['diff'].iloc[-1]
#                nav = nav['diff'].mean()
#                df = df.drop(columns='nav')
#                df['final_price'] = df['final_price'].fillna(method='ffill')
#                df = df.dropna()
#                df = df.reset_index()
#                for j in periodList:
#                    df[f'{j}'] = (df['final_price']/df['final_price'].shift(int(j)))-1
#                if onDate==False:
#                    df = df[df.index==df.index.max()]
#                else:
#                    df = df[df.index<=int(onDate)]
#                    df = df[df.index==df.index.max()]
#                if len(df)>0:
#                    dic = df.to_dict(orient='records')[0]
#                    if 'index' in dic:del dic['index']
#                    del dic["final_price"]
#                    df = pd.DataFrame(dic.items(),columns=['period','ptp'])
#                    df = df[df['period']!="dateInt"]
#                    df['periodint'] = [365/int(x) for x in periodList]
#                    df['yearly'] = round(((((df['ptp']+1)**df['periodint'])-1)*100),2)
#                    df['ptp'] = [round((x*100),2) for x in df['ptp']]
#                    df = df[['period','yearly']].fillna(0)
#                    df = pd.pivot_table(df,columns='period')
#                    df.index = [i.replace(' ','')]
#                    df['mean30PN'] = round(nav,2)
#                    df['navlast'] = round(navlast,2)
#                    dff = pd.concat([dff,df])
#            else:
#                df = pd.DataFrame(farasahmDb['fixIncomeHistori'].find({'name':i},{ 'dateInt':1, '_id':0,'close_price_change_percent':1,'final_price':1,'nav':1})).set_index('dateInt')
#                day_list = Day_list()
#                day_list =  filter(lambda x: x >= df.index.min(), day_list)
#                day_list =  filter(lambda x: x <= df.index.max(), day_list)
#                df = df.join(pd.DataFrame(index=day_list), how='right')
#                df = df.sort_index(ascending=True)
#                nav = df.iloc[-30:]
#                nav = df.iloc[-30:]
#                nav = nav[nav['nav']>0]
#                nav = nav[nav['nav']!=np.nan]
#                nav['diff'] = (nav['final_price'] / nav['nav'])-1
#                nav['diff'] = nav['diff'] * 100
#                navlast = nav['diff'].iloc[-1]
#                nav = nav['diff'].mean()
#                df = df.drop(columns=['nav','final_price'])
#                df = df.where(df>0,0)
#                df['close_price_change_percent'] = (df['close_price_change_percent']/100)+1
#                if onDate==False:
#                    df = df[df.index<=df.index.max()]
#                else:
#                    df = df[df.index<=int(onDate)]
#                    if len(df)==0:
#                        return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
#                df = df.reset_index()
#                dic ={}
#                for j in periodList:
#                    d = list(df[df.index>df.index.max()-j]['close_price_change_percent'])
#                    if len(d)==j:
#                        r = (np.prod(d))**(365/j)
#                        dic[f'{j}'] = [int((np.prod(d)-1)*10000)/100, np.nan, int((r-1)*10000)/100]
#                    else:
#                        dic[f'{j}'] = [np.nan, np.nan, np.nan]
#                df = pd.DataFrame.from_dict(dic,orient='index')
#                df = df.reset_index()
#                df.columns = ['period','ptp','periodint','yearly']
#                df = df[['period','yearly']]
#                df = pd.pivot_table(df,columns='period')
#                df.index = [i.replace(' ','')]
#            
#                df['mean30PN'] = 0#round(nav,2)
#                df['navlast'] = 0#round(navlast,2)
#
#                dff = pd.concat([dff,df])
#    if len(dff)==0:
#        return json.dumps({'replay':False, 'msg':'اطلاعاتی موجود نیست'})
#    else:
#        dff = dff.reset_index()
#        dff = dff.fillna(0)
#
#        dic ={'d1':float(dff['1'].max()),'d14':float(dff['14'].max()),'d180':float(dff['180'].max()),'d30':float(dff['30'].max()),'d365':float(dff['365'].max()),'d730':float(dff['730'].max()),'mean30PN':float(dff['mean30PN'].max()),'navlast':float(dff['navlast'].max())}
#        dff = dff.to_dict(orient='records')
#
#    return json.dumps({'replay':True,'df':dff,'dic':dic})


def getshareholders(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol},{'symbol':0}))
    lastUpdate = int(df['date'].max())
    df= df[df['date']==df['date'].max()]
    df = df[['نام و نام خانوادگی','کد ملی','نام پدر','تعداد سهام','شماره تماس','_id']]
    df = df.sort_values(by=['تعداد سهام'], ascending=False)
    df = df.drop_duplicates(subset=['کد ملی','نام و نام خانوادگی'])
    df['شماره تماس'] = df['شماره تماس'].fillna('')
    df['کد ملی'] = df['کد ملی'].fillna('')
    df['نام پدر'] = df['نام پدر'].fillna('')
    df['تعداد سهام'] = [int(x) for x in df['تعداد سهام']]
    df['کد ملی'] = [str(x) for x in df['کد ملی']]
    df['rate'] = df['تعداد سهام'] / df['تعداد سهام'].sum()
    df['rate'] = [int(x*10000)/100 for x in df['rate']]
    df['_id'] = [str(x) for x in df['_id']]
    df = df.fillna('')
    dic = {'amount':int(df['تعداد سهام'].max()),'lastUpdate':lastUpdate}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getgrouping(data):
    dff = farasahmDb['grouping'].find({'symbol':data['access'][1]},{'_id':0,'symbol':0,'user':0})
    totalStock = farasahmDb['companyBasicInformation'].find_one({'symbol':data['access'][1]})['تعداد سهام']
    totalStock = int(totalStock)
    df = []
    for i in dff:
        dic = {'name':i['nameGroup'],'سهام کل':0 ,'fullname':'','len':len(i['members']), '_children':[{'code':x,'name':i['nameGroup']} for x in i['members']]}
        for j in range(len(dic['_children'])):
            fullname = pd.DataFrame(farasahmDb['register'].find({"کد سهامداری":dic['_children'][j]['code'],'symbol':data['access'][1]},{'تاریخ گزارش':1,'_id':0,'سهام کل':1,'نام خانوادگی ':1,'نام':1}))
            if len(fullname)==0:
                fullname = farasahmDb['traders'].find_one({"کد":dic['_children'][j]['code'],'symbol':data['access'][1]})
                dic['_children'][j]['fullname'] = fullname['fullname']
                dic['_children'][j]['سهام کل'] = 0
                dic['_children'][j]['درصد مالکیت'] = 0
            else:
                fullname = fullname[fullname['تاریخ گزارش']==fullname['تاریخ گزارش'].max()].to_dict('records')[0]
                dic['_children'][j]['fullname'] = (str(fullname['نام']) +' '+ str(fullname['نام خانوادگی '])).replace('nan','')
                dic['_children'][j]['سهام کل'] =fullname['سهام کل']
                dic['_children'][j]['درصد مالکیت'] = int((int(fullname['سهام کل']) / (totalStock))*10000)/100
                dic['سهام کل'] = dic['سهام کل'] + int(fullname['سهام کل'])
        dic['درصد مالکیت'] = int((int(dic['سهام کل']) / totalStock)*10000)/100
        df.append(dic)
    if len(df) == 0:
        dic = {'stock':0}
    dic = {'stock':max([x['سهام کل'] for x in df])}
    return json.dumps({'replay':True,'df':df,'dic':dic})



def getallnameplus(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol,},{'_id':0,'کد خریدار':1,'نام خانوادگی خریدار':1,'نام خریدار':1,'محل صدور خریدار':1,'کد ملی خریدار':1,'کد فروشنده':1,'نام خانوادگی فروشنده':1,'نام فروشنده':1,'محل صدور فروشنده':1,'کد ملی فروشنده':1}))
    if len(df) == 0 :return json.dumps({'replay':False})
    dfb = df[['کد خریدار','نام خانوادگی خریدار','نام خریدار','محل صدور خریدار','کد ملی خریدار']]
    dfs = df[['کد فروشنده','نام خانوادگی فروشنده','نام فروشنده','محل صدور فروشنده','کد ملی فروشنده']]
    dfb.columns = ['کد','نام خانوادگی','نام','محل صدور','کد ملی']
    dfs.columns = ['کد','نام خانوادگی','نام','محل صدور','کد ملی']
    df = pd.concat([dfs,dfb])
    df = df.drop_duplicates()
    df = df.fillna('')
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})


def getprofiletrader(data):
    symbol = data['access'][1]
    df = (farasahmDb['register'].find_one({'symbol':symbol,'کد سهامداری':data['code']},{'_id':0,'نماد':0,'نماد کدال':0},sort=[( "تاریخ گزارش", -1 )]))
    for k,v in df.items():
        if str(v) == 'nan': df[k] = ''
    return json.dumps({'replay':True,'df':df})


def getbalancetrader(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['register'].find({'symbol':symbol,'کد سهامداری':data['trader']},{'_id':0,'سهام کل':1,'تاریخ گزارش':1})).drop_duplicates(subset='تاریخ گزارش')
    dfListDate = pd.DataFrame(farasahmDb['register'].find({'symbol':symbol},{'_id':0,'تاریخ گزارش':1})).drop_duplicates()
    dfListDate = dfListDate[dfListDate['تاریخ گزارش']>=df['تاریخ گزارش'].min()]
    dfListDate['realDate'] = 1
    df = df.set_index('تاریخ گزارش').join(dfListDate.set_index('تاریخ گزارش'),how='outer')
    df = df.fillna(0).sort_index().reset_index()[['تاریخ گزارش','سهام کل']]
    df = df[df['تاریخ گزارش']>14011222]
    df.columns = ['time','value']
    df['time'] = [dateIntToDateGorgiaStr(x) for x in df['time']]
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})

def getbroker(data):
    symbol = data['access'][1]
    date = datetime.datetime.fromtimestamp(data['date']/1000)
    date = int(str(JalaliDate.to_jalali(date.year, date.month, date.day)).replace('-',''))
    df = pd.DataFrame(farasahmDb['broker'].find({'symbol':symbol,'date':date},{'symbol':0,'_id':0}))
    if len(df)==0:
        return json.dumps({'replay':False})
    station = pd.DataFrame(farasahmDb['station'].find({'symbol':symbol,'date':date},{'symbol':0,'_id':0}))
    brokerList = [x for x in farasahmDb['borkerList'].find({},{'_id':0})]
    station['broker'] = ''
    for s in station.index:
        for b in brokerList:
            name = list(b.keys())[0]
            members = b[name]
            for m in members:
                if m in station['نام کارگزار'][s]:
                    station['broker'][s] = name
                    break
                    break
    station['broker'] = station['broker'].replace('','نامشخص')
    dic = {'cntBuy':int(df['تعداد خرید'].max()),'cntSel':int(df['تعداد فروش'].max())}
    df['_children'] = ''
    df = df.to_dict('records')
    for d in range(len(df)):
        station_ = station[station['broker']==df[d]['broker']].to_dict('records')
        for s in station_:
            df[d]['_children'] = {'station':s['نام کارگزار'],'تعداد خرید':s['تعداد خرید'],'تعداد فروش':s['تعداد فروش'],'قیمت خرید':s['قیمت خرید'],'قیمت فروش':s['قیمت فروش']}

    return json.dumps({'replay':True,'df':df, 'dic':dic})


def getpersonalnobourse(data):
    symbol = data['access'][1]
    personal = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol,'نام و نام خانوادگی':data['name']},{'_id':0}))
    if len(personal) ==0: return json.dumps({'replay':False,'msg':'سرمایه گذاری یافت نشد'})
    personal = personal.fillna(0)
    personal = personal[personal['date']==personal['date'].max()].to_dict('records')[0]
    return json.dumps({'replay':True,'personal':personal})

def gettransactions(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['transactions'].find({'symbol':symbol},{'_id':0,'balanceSell':0,'balanceBuy':0,'symbol':0}))
    dic = {'vol':int(df['volume'].max()),'val':int(df['value'].max()),'prc':int(df['price'].max())}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getinformationcompany(data):
    symbol = data['access'][1]
    dic = farasahmDb['companyBasicInformation'].find_one({'symbol':symbol},{'_id':0,'symbol':0})
    if dic == None:
        return json.dumps({'replay':True,'dic':{}})
    return json.dumps({'replay':True,'dic':dic})


def gettraderactivityreport(data):
    symbol = data['access'][1]
    trader = data['trader']
    tradeListDate = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol},{'_id':0,'تاریخ معامله':1,'قیمت هر سهم':1,'تعداد سهم':1}))
    tradeListDate['value'] = tradeListDate['قیمت هر سهم'] * tradeListDate['تعداد سهم']
    tradeListDate = tradeListDate.groupby(by=['تاریخ معامله']).sum().reset_index()
    tradeListDate.columns = ['date', 'volume', 'قیمت هر سهم', 'value']
    tradeListDate = tradeListDate[['date', 'volume', 'value']]
    dfbuy = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol,'کد خریدار':trader},{'_id':0,'تاریخ معامله':1,'تعداد سهم':1,'قیمت هر سهم':1}))
    dfsell = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol,'کد فروشنده':trader},{'_id':0,'تاریخ معامله':1,'تعداد سهم':1,'قیمت هر سهم':1}))
    if len(dfbuy)>0: dfbuy.columns = ['date', 'volume_buy', 'price_buy']
    if len(dfsell)>0: dfsell.columns = ['date', 'volume_sell', 'price_sell']
    df = pd.concat([dfbuy,dfsell])
    tradeListDate = tradeListDate[tradeListDate['date']>= df['date'].min()]
    df = pd.concat([df,tradeListDate]).fillna(0)
    if len(dfbuy)==0:
        df['volume_buy'] = 0
        df['price_buy'] = 0
    if len(dfsell)==0:
        df['volume_sell'] = 0
        df['price_sell'] = 0
    df['value_buy'] = df['volume_buy'] * df['price_buy']
    df['value_sell'] = df['volume_sell'] * df['price_sell']
    df = df.drop(columns=['price_buy','price_sell'])
    df = df.groupby(by=['date']).sum().sort_index()
    df['price_buy'] = df['value_buy'].expanding(1).sum() / df['volume_buy'].expanding(1).sum()
    df['price_sell'] = df['value_sell'].expanding(1).sum() / df['volume_sell'].expanding(1).sum()
    df['price_buy'] = df['price_buy'].fillna(0)
    df['price_sell'] = df['price_sell'].fillna(0)
    df['price_buy'] = [int(x) for x in df['price_buy']]
    df['price_sell'] = [int(x) for x in df['price_sell']]
    df['price'] = df['value'] / df['volume']
    df = df.drop(columns=['volume','value'])
    df['price'] = [int(x) for x in df['price']]
    df = df.sort_index(ascending=False).reset_index()
    dic = {'volume_buy':df['volume_buy'].max(),'volume_sell':df['volume_sell'].max()}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getbrokeractivityreport(data):
    symbol = data['access'][1]
    broker = data['broker']
    tradeListDate = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol},{'_id':0,'تاریخ معامله':1,'قیمت هر سهم':1,'تعداد سهم':1}))
    tradeListDate['value'] = tradeListDate['قیمت هر سهم'] * tradeListDate['تعداد سهم']
    tradeListDate = tradeListDate.groupby(by=['تاریخ معامله']).sum().reset_index()
    tradeListDate.columns = ['date', 'volume', 'قیمت هر سهم', 'value']
    tradeListDate = tradeListDate[['date', 'volume', 'value']]
    df = pd.DataFrame(farasahmDb['broker'].find({'symbol':symbol,'broker':broker},{'نام کارگزار':0,'broker':0,'_id':0,'symbol':0}))
    tradeListDate = tradeListDate[tradeListDate['date']>= df['date'].min()]
    df = pd.concat([df,tradeListDate]).fillna(0)
    df = df.groupby(by=['date']).sum()
    df = df.sort_index()
    df['price'] = df['value'] / df['volume']
    df = df.drop(columns=['value','volume'])
    df['volume_balance'] = df['تعداد خرید'] - df['تعداد فروش']
    df['volume_balance'] = df['volume_balance'].expanding(1).sum()
    df = df.sort_index(ascending=False).reset_index()
    dic = {'volume_balance':df['volume_balance'].max(),'تعداد خرید':df['تعداد خرید'].max(),'تعداد فروش':df['تعداد فروش'].max()}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getstationactivityreport(data):
    symbol = data['access'][1]
    station = data['station']
    tradeListDate = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol},{'_id':0,'تاریخ معامله':1,'قیمت هر سهم':1,'تعداد سهم':1}))
    tradeListDate['value'] = tradeListDate['قیمت هر سهم'] * tradeListDate['تعداد سهم']
    tradeListDate = tradeListDate.groupby(by=['تاریخ معامله']).sum().reset_index()
    tradeListDate.columns = ['date', 'volume', 'قیمت هر سهم', 'value']
    tradeListDate = tradeListDate[['date', 'volume', 'value']]
    df = pd.DataFrame(farasahmDb['station'].find({'symbol':symbol,'نام کارگزار':station},{'نام کارگزار':0,'_id':0,'symbol':0}))
    tradeListDate = tradeListDate[tradeListDate['date']>= df['date'].min()]
    df = pd.concat([df,tradeListDate]).fillna(0).groupby(by=['date']).sum().sort_index()
    df['price'] = df['value'] / df['volume']
    df = df.drop(columns=['value','volume'])
    df['volume_balance'] = df['تعداد خرید'] - df['تعداد فروش']
    df['volume_balance'] = df['volume_balance'].expanding(1).sum()
    df = df.sort_index(ascending=False).reset_index()
    dic = {'volume_balance':df['volume_balance'].max(),'تعداد خرید':df['تعداد خرید'].max(),'تعداد فروش':df['تعداد فروش'].max()}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def excerpttrader(data):
    symbol = data['access'][1]
    trader = data['trader']
    dfbuy = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol,'کد خریدار':trader},{'_id':0,'تاریخ معامله':1,'تعداد سهم':1,'قیمت هر سهم':1,'نام کارگزار خریدار':1}))
    dfsell = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol,'کد فروشنده':trader},{'_id':0,'تاریخ معامله':1,'تعداد سهم':1,'قیمت هر سهم':1,'نام کارگزار فروشنده':1}))
    if len(dfbuy)>0: dfbuy.columns = ['date', 'volume_buy', 'price_buy', 'station']
    else: dfbuy = pd.DataFrame(columns=['date', 'volume_buy', 'price_buy', 'station'])
    if len(dfsell)>0: dfsell.columns = ['date', 'volume_sel', 'price_sel', 'station']
    else: dfsell = pd.DataFrame(columns=['date', 'volume_sel', 'price_sel', 'station'])
    df = pd.concat([dfbuy,dfsell]).fillna(0)
    df = df[df['date']!=0]
    stations = list(set(df['station'].to_list()))[:4]
    df['value_buy'] = df['volume_buy']*df['price_buy']
    df['value_sel'] = df['volume_sel']*df['price_sel']
    df = df.drop(columns=['station','price_buy','price_sel'])
    df = df.groupby('date').sum().reset_index()
    toDay = JalaliDate.today()
    periodList = periodListGenerate()
    tradeListDate = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol},{'_id':0,'تاریخ معامله':1,'قیمت هر سهم':1,'تعداد سهم':1}))
    tradeListDate['value'] = tradeListDate['قیمت هر سهم'] * tradeListDate['تعداد سهم']
    tradeListDate = tradeListDate.groupby(by=['تاریخ معامله']).sum().reset_index()
    tradeListDate.columns = ['date', 'volume', 'قیمت هر سهم', 'value']
    tradeListDate = tradeListDate[['date', 'volume', 'value']]
    price_bef = tradeListDate[tradeListDate['date']==tradeListDate['date'].min()]
    price_bef = price_bef['value'][price_bef.index.min()] / price_bef['volume'][price_bef.index.min()]
    tradeListDate = tradeListDate[tradeListDate['date']>= df['date'].min()]
    df = pd.concat([df,tradeListDate])
    df = df.groupby('date').sum().sort_index()
    df['price'] = df['value'] / df['volume']
    df = df.drop(columns=['value','volume'])
    df = df.fillna(0)
    volume_bal = farasahmDb['register'].find_one({'symbol':symbol,'کد سهامداری':trader,'تاریخ گزارش':int(df.index.max())})
    if volume_bal == None: volume_bal = 0
    else: volume_bal = volume_bal['سهام کل']
    value_bal = volume_bal * df['price'][df.index.max()]
    volume_buy = df['volume_buy'].sum()
    volume_sel = df['volume_sel'].sum()
    value_buy = df['value_buy'].sum()
    value_sel = df['value_sel'].sum()
    volume_bef = (volume_bal - volume_buy) + volume_sel
    value_bef = price_bef * volume_bef
    if volume_bef>0 :price_bef = value_bef / volume_bef
    else:price_bef = 0
    price_buy = value_buy / volume_buy
    price_sel = value_sel / volume_sel
    if str(price_buy)=='nan': price_buy = 0
    if str(price_sel)=='nan': price_sel = 0
    profit = (value_sel + value_bal) - (value_bef + value_buy)
    profit = int(profit / 1000000)
    profitRate = (value_sel + value_bal) / (value_bef + value_buy)
    profitRate = int((profitRate - 1) * 100)
    fee = {'buy':int((value_buy*0.003632)/1000000),'sell':int((value_sel*0.0088)/1000000)}
    status = {'balance':volume_bal,'volume_buy':volume_buy,'volume_sel':volume_sel,'volume_bef':volume_bef,'price_bef':price_bef,'price_bal':int(df['price'][df.index.max()]),'price_buy':int(price_buy),'price_sel':int(price_sel),'profit':profit,'profitRate':profitRate}
    dic = {'fee':fee,'stations':stations,'status':status}
    LenDffPeriod = 0
    for i in periodList:
        prid = i['period']
        dt = i['date']
        dff = df[df.index>=dt]
        if len(dff)>LenDffPeriod:
            LenDffPeriod = len(dff)
            volume_buy = dff['volume_buy'].sum()
            volume_sel = dff['volume_sel'].sum()
            value_buy = dff['value_buy'].sum()
            value_sel = dff['value_sel'].sum()
            volume_bef = volume_bal - volume_buy + volume_sel
            price_bef = dff['price'][dff.index.min()]
            value_bef = volume_bef * price_bef
            price_buy = value_buy / volume_buy
            price_sel = value_sel / volume_sel
            if str(price_buy)=='nan': price_buy = 0
            if str(price_sel)=='nan': price_sel = 0

            profit = (value_sel + value_bal) - (value_bef + value_buy)
            if profit == 0:
                profitRate = 0
            else:
                profit = int(profit / 1000000)
                profitRate = (value_sel + value_bal) / (value_bef + value_buy)
                profitRate = int((profitRate - 1) * 100)
            status = {'avalibale':True,'balance':volume_bal,'volume_buy':volume_buy,'volume_sel':volume_sel,'volume_bef':volume_bef,'price_bef':int(price_bef),'price_bal':int(df['price'][df.index.max()]),'price_buy':int(price_buy),'price_sel':int(price_sel),'profit':profit,'profitRate':profitRate}
            dic[f'status_{prid}'] = status
        else:
            dic[f'status_{prid}'] = {'avalibale':False}
    return json.dumps({'replay':True,'dic':dic})



def getformerstockman(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['register'].find({'symbol':symbol}))
    return json.dumps({'replay':True,'dic':'dic'})


def getreportmetric(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['features'].find({'symbol':symbol},{'_id':0,'symbol':0}))
    '''
    نوسان : priceFluctuation=> std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50) * 127.04096041
    اجماع فروشندگان : consensusSaller=> avg(count(seller),50) * -366.39014197
    ترس : fear=> max(count(seller),5)/min(count(seller),5) * -0.76806955
    کف 20 روزه : minimum20=> min(price,20)/price * -186.10641997
    '''
    df = df[['date','std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50)',
             'avg(count(seller),50)','max(count(seller),5)/min(count(seller),5)',
             'min(price,20)/price']]
    df = df.fillna(0)
    
    df = df.rename(columns={"std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50)":'priceFluctuation'})
    df = df.rename(columns={"avg(count(seller),50)":'consensusSaller'})
    df = df.rename(columns={"max(count(seller),5)/min(count(seller),5)":'fear'})
    df = df.rename(columns={"min(price,20)/price":'minimum20'})
    # weg
    df['priceFluctuation'] = df['priceFluctuation'] * 127.04096041
    df['consensusSaller'] = df['consensusSaller'] * -366.39014197
    df['fear'] = df['fear'] * -0.76806955
    df['minimum20'] = df['minimum20'] * -0.76806955
    # z normaltion
    df['priceFluctuation'] = (df['priceFluctuation'] - df['priceFluctuation'].mean()) / df['priceFluctuation'].std()
    df['consensusSaller'] = (df['consensusSaller'] - df['consensusSaller'].mean()) / df['consensusSaller'].std()
    df['fear'] = (df['fear'] - df['fear'].mean()) / df['fear'].std()
    df['minimum20'] = (df['minimum20'] - df['minimum20'].mean()) / df['minimum20'].std()
    df['minimum20'] = (df['minimum20'] - df['minimum20'].mean()) / df['minimum20'].std()

    df = df.fillna(0).sort_values(by=['date'],ascending=False)
    dic = {"priceFluctuation":df['priceFluctuation'].max(), 'consensusSaller':df['consensusSaller'].max(),
           "fear":df['fear'].max(), 'minimum20':df['minimum20'].max(),
           }
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})



def getassembly(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['assembly'].find({'symbol':symbol}))
    if len(df)==0:
        return json.dumps({'replay':False,'msg':'مجمع یافت نشد'})
    df['date'] = [str(JalaliDate(x)) for x in df['date']]  
    df['_id'] = [str(x) for x in df['_id']]  
    
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})



# نا قص
def getpersonaldata(data):
    symbol = data['access'][1]
    companyType = farasahmDb['companyList'].find_one({'symbol':symbol},{'type':1,'_id':0})['type']
    if companyType == 'Bourse':
        lastupdate = farasahmDb['register'].find_one({},sort=[('تاریخ گزارش',-1)])['تاریخ گزارش']
        register = farasahmDb['register'].find_one({'کد ملی':int(data['idPersonal']),'تاریخ گزارش':int(lastupdate)},{'_id':0})
        if register == None:
            return json.dumps({'replay':False,'msg':'فرد در حال حاضر سهامدار نیست'})
        else:
            return json.dumps({'replay':True,'df':register})

    return json.dumps({'replay':True,'df':'df'})


def personalinassembly(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['personalAssembly'].find({'symbol':symbol},{'_id':0,'کد سهامداری':1,'سهام کل':1,'fullName':1,'کد ملی':1,'تاریخ تولد':1,'نام پدر':1,'محل صدور':1}))
    total = farasahmDb['companyBasicInformation'].find_one({'symbol':symbol},{'_id':0,'تعداد سهام':1})['تعداد سهام']
    if len(df)==0:
        return json.dumps({'replay':False,'msg':'فردی به حاضرین مجمع افزوده نشده'})
    df['rate'] = (df['سهام کل'] / int(total)) *100
    df['rate'] = [int(x*1000)/1000 for x in df['rate']]
    votes = pd.DataFrame(farasahmDb['votes'].find({'symbol':symbol,'type':'controller'},sort=[('date',-1)]))
    if len(votes)>0:
        votes = votes[['nc','opt']]
        votes = votes.drop_duplicates(subset=['nc'])
        votes = votes.rename(columns={'nc':'کد ملی'})
        votes = votes.set_index('کد ملی')
        df = df.set_index('کد ملی')
        df = df.join(votes).reset_index()
        df['opt'] = df['opt'].fillna('')
    else:
        df['opt'] = ''
    df = df.sort_values(by=['rate'],ascending=False)
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})


def getsheetassembly(data):
    symbol = data['symbol']
    nc = data['nc']
    assembly = farasahmDb['assembly'].find_one({'symbol':symbol},sort=[('date',-1)])
    if assembly ==None: return json.dumps({'replay':False,'msg':'هیچ مجمعی یافت نشد'})
    del assembly['_id']
    del assembly['date']
    company = farasahmDb['companyList'].find_one({'symbol':symbol},{'type':1,'_id':0,'fullname':1})
    companyType = company['type']
    if companyType == 'Bourse':
        lastupdate = farasahmDb['register'].find_one({},sort=[('تاریخ گزارش',-1)])['تاریخ گزارش']
        register = farasahmDb['register'].find_one({'کد ملی':int(nc),'تاریخ گزارش':int(lastupdate)},{'_id':0})
        if register == None:
            return json.dumps({'replay':False,'msg':'فرد در حال حاضر سهامدار نیست'})
    response = {'register':register,'assembly':assembly,'company':company}
    return json.dumps({'replay':True,'data':response})


def getdatavotes(data):
    symbol = data['access'][1]
    assembly = farasahmDb['assembly'].find_one({'symbol':symbol},sort=[('date',-1)])

    del assembly['_id']
    del assembly['date']

    return json.dumps({'replay':True,'data':assembly})


def addvoteasemboly(data):
    symbol = data['access'][1]
    vote = data['controllerVotes']
    vote['date'] = int(str(JalaliDate(datetime.datetime.now())).replace('-',''))
    vote['symbol'] = symbol
    vote['type'] = 'controller'
    if farasahmDb['votes'].find_one({'nc':vote['nc'],'date':vote['date'],'symbol':symbol,'type':vote['type']}) != None:
        farasahmDb['votes'].update_one({'nc':vote['nc'],'date':vote['date'],'symbol':symbol,'type':vote['type']},{'$set':{'opt':vote['opt']}})
    else:
        farasahmDb['votes'].insert_one(vote)
    return json.dumps({'replay':True})

def getresultvotes(data):
    df = pd.DataFrame(farasahmDb['votes'].find({'symbol':data['symbol'],'type':'controller'},{'_id':0}))
    dff = pd.DataFrame(farasahmDb['personalAssembly'].find({'symbol':data['symbol']},sort=[('تاریخ گزارش',-1)]))[['کد ملی','سهام کل']]
    allVote = dff['سهام کل'].sum()
    df = df.set_index('nc').join(dff.set_index('کد ملی'))
    df = df[['opt','سهام کل']].groupby('opt').sum()
    df = df.sort_values(by=['سهام کل'],ascending=False).reset_index()
    df['status'] = '-'
    df['status'][0] = 'منتخب'
    df['status'][1] = 'علی البدل'
    df = pd.concat([df,pd.DataFrame([{'opt':'باطله','سهام کل':dff['سهام کل'].sum()-df['سهام کل'].sum(),'status':'-'}])])
    df = df.to_dict('records')
    company = farasahmDb['companyList'].find_one({'symbol':data['symbol']},{'type':1,'_id':0,'fullname':1})
    return json.dumps({'replay':True, 'df':df,'company':company})

def getcapitalincrease(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['capitalIns'].find({'symbol':symbol}))
    df['_id'] = df['_id'].astype(str)
    df = df.to_dict('records')
    return json.dumps({'replay':True, 'df':df})

def getpriority(data):
    symbol = data['access'][1]
    datePriority = data['datePriority']
    df = pd.DataFrame(farasahmDb['Priority'].find({'symbol':symbol,'تاریخ':datePriority},{'enable':0}))
    dfPay = pd.DataFrame(farasahmDb['PriorityPay'].find({"symbol":symbol,'capDate':datePriority},{'enable':0}))
    if len(dfPay)>0:
        dfPay = dfPay.groupby(by=['frm']).sum(numeric_only=True)
        df = df.set_index('نام و نام خانوادگی').join(dfPay).fillna(0)
    else:
        df['popUp'] = 0
        df['value'] = 0

    df = df.rename(columns={"popUp":"تعداد واریز","value":"ارزش واریز"})
    df = df.reset_index()
    df['_id'] = df['_id'].astype(str)
    df = df.fillna(0)
    df['countForward'] = df['حق تقدم استفاده شده'] + df['تعداد سهام']
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})



def getprioritytransaction(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['PriorityTransaction'].find({'symbol':symbol, 'capDate':data['date']},{'popUp':0,'symbol':0}))
    if len(df)==0:
        return json.dumps({'replay':False,'msg':'تراکنشی یافت نشد'})
    df['_id'] = df['_id'].apply(str)
    df['date'] = df['date'].apply(JalaliDate.to_jalali)
    df['date'] = df['date'].apply(str)
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})



def preemptioncard(data):
    dt = farasahmDb['Priority'].find_one({'symbol':data['sym'],'کد ملی':data['nc'],'تاریخ':"1402/10/13"},{'_id':0,'تعداد سهام':0})
    if dt == None:return json.dumps({'replay':False})
    return json.dumps({'replay':True})



def preemptioncardjpg(data):
    dt = farasahmDb['Priority'].find_one({'symbol':data['sym'],'کد ملی':data['nc'], 'تاریخ':"1402/10/13"},{'_id':0})
    saham = farasahmDb['registerNoBours'].find_one({'کد ملی':data['nc'],'symbol':data['sym']})
    if dt == None:return json.dumps({'replay':False})
    a4_width, a4_height = int(210 * 3.779527559), int(297 * 3.779527559)
    a4_image = Image.new("RGB", (a4_width, a4_height), "white")
    logo_path = "public/logo.jpg"
    logo_image = Image.open(logo_path)
    logo_width_new = int(a4_width * 0.2)
    logo_height_new = int(logo_image.height * (logo_width_new / logo_image.width))
    logo_image = logo_image.resize((logo_width_new, logo_height_new))
    x_position_logo = (a4_width - logo_width_new) // 2
    y_position_logo = 30  
    a4_image.paste(logo_image, (x_position_logo, y_position_logo))
    draw = ImageDraw.Draw(a4_image)
    textRow1 = "سهامدار گرامی " + dt['نام و نام خانوادگی'] + " با کد/شناسه ملی " + digits.en_to_fa(str(dt['کد ملی'])) + ' دارای تعداد ' + digits.en_to_fa(str(dt['تعداد سهام'])) + 'سهم'
    textRow1 = arabic_reshaper.reshape(textRow1)
    textRow1 = get_display(textRow1)
    font_size = 15
    font_path = "public/Peyda-Medium.ttf" 
    font = ImageFont.truetype(font_path, font_size)
    text_width, text_height = draw.textsize(textRow1, font=font)
    x_position_text = (a4_width - text_width) - 10
    y_position_text = y_position_logo + logo_height_new + 25 
    text_color = (0, 0, 0)
    draw.text((x_position_text, y_position_text), textRow1, fill=text_color, font=font)
    textRow2 = 'بنا به تصمیم هیات مدیره شرکت و تفویض اختیار مصوبه ی مجمع فوق العاده مبنی بر افزایش سرمایه شرکت' + '\n'
    textRow2 = textRow2 + 'از' + digits.en_to_fa('5293454025000')+ ' ريال ،'
    textRow2 = textRow2 + 'پنج هزار و دویست و نود و سه میلیارد و چهارصد و پنجاه و چهار میلیون و پانصد و هزار و دویست و پنجاه' +'\n' #digits.to_word(5293454025000) + '\n'
    textRow2 = textRow2 + ' به ' + digits.en_to_fa('8257788279000') + 'هشت هزار و دویست و پنجاه و هفت میلیارد و هفتصد و هشتاد و هشت میلیون و دویست و هفتاد '+ '\n' +' و هشت هزار و دویست و نود' + ' ریال ' + '\n' #+  digits.to_word(8257788279000) + ' ریال ' + '\n'
    textRow2 = textRow2 + ' شما میتوانید از حق تقدم خود معادل ' + digits.en_to_fa(str(dt['حق تقدم'])) + ' سهم به ارزش اسمی \n' 
    textRow2 = textRow2 + digits.en_to_fa('1000') + ' ریال استفاده نمایید.'  + ' لذا خواهشمند است با توجه به مصوبات موجود که مبلغ ' + digits.en_to_fa(str(int(dt['حق تقدم'])*1000)) + ' ریال به صورت آورده' + '\n'
    textRow2 = textRow2 + ' نقدی طبق اعلامیه پذیرش نویسی در روز نامه پیمان یزد به شماره 5848 مورخ 1402/10/14 و به مدت ' + digits.en_to_fa('60') 
    textRow2 = textRow2 + '\n' + ' روز نسبت به استفاده' + ' حق تقدم و پذیره نویسی تا تاریخ 1402/12/13 اقدام نمایید.'
    textRow2 = arabic_reshaper.reshape(textRow2)
    textRow2 = get_display(textRow2)
    font = ImageFont.truetype(font_path, font_size)
    text_width, text_height = draw.textsize(textRow2, font=font)
    x_position_text = (a4_width - text_width) - 30
    y_position_text = y_position_logo + logo_height_new + 55
    draw.text((x_position_text, y_position_text), textRow2, fill=text_color, font=font,align='right')
    textRow3 = 'بنابر این مبلغ ....................... ريال بر اساس فیش نقدی به شماره ................بانک توسعه صادرات حساب شماره \n 0200054690004 و شماره شبای IR270200000000200054690004 به نام شرکت صنایع مفتول ایساتیس پویا \n به پیوست ارائه میگردد.'
    textRow3 = arabic_reshaper.reshape(textRow3)
    textRow3 = get_display(textRow3)
    text_width, text_height = draw.textsize(textRow3, font=font)
    x_position_text = (a4_width - text_width) - 50
    y_position_text = y_position_logo + logo_height_new + 210
    draw.text((x_position_text, y_position_text), textRow3, fill=text_color, font=font,align='right')
    textRow3 = 'امضا'
    textRow3 = arabic_reshaper.reshape(textRow3)
    textRow3 = get_display(textRow3)
    text_width, text_height = draw.textsize(textRow3, font=font)
    x_position_text = (a4_width - 100)
    y_position_text = y_position_logo + logo_height_new + 240
    draw.text((200, y_position_text), textRow3, fill=text_color, font=font)
    textRow3 = 'اینجانب استفاده از حق تقدم خود جهت شرکت در افزایش  سرمایه به آقا/خانم/شرکت ....................... واگذار میکنم\nکه تصاویر مدارک هویتی نامبرده به پیوست ارائه میگردد. '
    textRow3 = arabic_reshaper.reshape(textRow3)
    textRow3 = get_display(textRow3)
    text_width, text_height = draw.textsize(textRow3, font=font)
    x_position_text = (a4_width - text_width) - 50
    y_position_text = y_position_logo + logo_height_new + 290
    draw.text((x_position_text, y_position_text), textRow3, fill=text_color, font=font,align='right')
    textRow4 = 'امضای استفاده کننده حق تقدم' + (' '*120) +'امضای سهامدار'
    textRow4 = arabic_reshaper.reshape(textRow4)
    textRow4 = get_display(textRow4)
    text_width, text_height = draw.textsize(textRow4, font=font)
    x_position_text = (a4_width - text_width) /2
    y_position_text = y_position_logo + logo_height_new + 380
    draw.text((x_position_text, y_position_text), textRow4, fill=text_color, font=font,align='right')
    font_size = 11
    font = ImageFont.truetype(font_path, font_size)
    textRow4 = 'لطفا با تکمیل نمودن فرم، آن را به امور سهام شرکت واقع در بلوار جمهوری ساختمان آنا طبقه 6 واحد 61 تحویل نمایید\nجهت کسب اطلاعات بیشتر با شماره 35233366-035'
    textRow4 = arabic_reshaper.reshape(textRow4)
    textRow4 = get_display(textRow4)
    text_width, text_height = draw.textsize(textRow4, font=font)
    x_position_text = (a4_width - text_width) -20
    y_position_text = y_position_logo + logo_height_new + 430
    draw.text((x_position_text, y_position_text), textRow4, fill=text_color, font=font,align='right')
    #dic = {'nc':data['nc'],'name':dt['نام و نام خانوادگی'],'symbol':data['sym'],'date':datetime.datetime.now()}
    #farasahmDb['GetCardPreemption'].insert_many(dic)
    a4_image.save("public/final_a4_image.png")
    return send_file("public/final_a4_image.png", as_attachment=True, mimetype="image/png")


def preemptioncardpdf(data):
    preemptioncardjpg(data)
    image = Image.open('public/final_a4_image.png')

    page_width, page_height = A4
    image_width, image_height = image.size
    scale = min(page_width / image_width, page_height / image_height)
    image_width_scaled = image_width * scale
    image_height_scaled = image_height * scale
    pdf = canvas.Canvas('public/final_a4_image.pdf', pagesize=A4)
    pdf.drawImage('public/final_a4_image.png', 0, 0, width=image_width_scaled, height=image_height_scaled)
    pdf.save()
    return send_file("public/final_a4_image.pdf", as_attachment=True)


def getxlsxpriority(data):
    symbol = data['access'][1]
    date = farasahmDb['capitalIns'].find_one({'_id':ObjectId(data['id'])})['date']
    df = pd.DataFrame(farasahmDb['Priority'].find({'symbol':symbol,'تاریخ':date}))
    df = df[df['کد ملی']!='99']
    df['url'] = 'farasahm.fidip.ir/pbl/pc/'+ symbol +'/'+ df['کد ملی'].astype(str)
    df = df.rename(columns={'نام و نام خانوادگی':'fullName','کد ملی':'nationalCode','نام پدر':'fathersName','حق تقدم':'countPriority','تاریخ':'date','حق تقدم استفاده شده':'usedCountPriority'})
    df.to_excel('public/df.xlsx',index=False)
    return send_file("public/df.xlsx", as_attachment=True)



def desk_broker_volumeTrade(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'replay':False})
    dateTo = data['date'][1]
    dateFrom = data['date'][0]
    df = pd.DataFrame(farasahmDb['deskBrokerVolumeTrade'].find({},{'_id':0}))
    df = df.replace(np.inf,0)
    df = df.replace(-np.inf,0)
    df = df.fillna(0)
    meanrate = {'نسبت کل':df['نسبت کل'].mean(), 'نسبت اوراق':df['نسبت اوراق'].mean(), 'نسبت صندوق':df['نسبت صندوق'].mean(), 'نسبت سهام':df['نسبت سهام'].mean()}
    if dateFrom != None and dateTo != None:
        dateFrom = Fnc.timestumpToJalalInt(dateFrom)
        dateTo = Fnc.timestumpToJalalInt(dateTo)
        df = df[df['date']<=dateTo]
        df = df[df['date']>=dateFrom]
    elif dateFrom != None:
        dateFrom = Fnc.timestumpToJalalInt(dateFrom)
        df = df[df['date']>=dateFrom]
    elif dateTo != None:
        dateTo = Fnc.timestumpToJalalInt(dateTo)
        df = df[df['date']<=dateTo]
    dic = {}
    df = df.fillna(0)
    df = df.set_index('date')
    for c in df.columns:
        if c in ['نسبت کل','نسبت اوراق','نسبت صندوق','نسبت سهام']:
            #df[c] = df[c].apply(Fnc.floatTo2decimal)
            df[f'{c} انحراف'] = df[c] - meanrate[c]
            df[f'{c} انحراف'] = df[f'{c} انحراف'].apply(Fnc.floatTo2decimalNormal)
        else:
            df[c] = df[c].apply(Fnc.toBillionRial)
    df.to_excel('ers.xlsx')
    for c in df.columns:
        dic[c] = float(df[c].max())
    df = df.reset_index()
    df = df.sort_values(by='date',ascending=False)
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})




def desk_broker_dateavalibale(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'replay':False})
    dataList = farasahmDb['TradeListBroker'].distinct('dateInt')
    dataList = [int(x) for x in dataList]
    lastDate = str(max(dataList))
    dataList = [str(x) for x in dataList]
    lastDate = JalaliDate(int(lastDate[:4]), int(lastDate[4:6]), int(lastDate[6:8])).to_gregorian()
    weekDate = lastDate - datetime.timedelta(days=7)
    lastDate = str(JalaliDate.to_jalali(lastDate.year, lastDate.month, lastDate.day))
    weekDate = str(JalaliDate.to_jalali(weekDate.year, weekDate.month, weekDate.day))
    return json.dumps({'dataList':dataList,'lastDate':lastDate,'weekDate':weekDate})

def datenow(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'replay':False})
    fromDate = datetime.datetime.now()
    toDate = fromDate + datetime.timedelta(days=5)
    lastDate = str(JalaliDate.to_jalali(toDate.year, toDate.month, toDate.day))
    weekDate = str(JalaliDate.to_jalali(fromDate.year, fromDate.month, fromDate.day))
    return json.dumps({'lastDate':lastDate,'weekDate':weekDate})
    

def desk_broker_gettraders(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'replay':False})
    symbol = farasahmDb['menu'].find_one({'name':data['access'][1]})['symbol']
    date = Fnc.timestumpToJalalInt(data['date'])
    farasahmDb['TradeListBroker'].create_index([("dateInt", pymongo.ASCENDING), ("TradeSymbol", pymongo.ASCENDING)])
    df = pd.DataFrame(farasahmDb['TradeListBroker'].find(
        {'dateInt':date,'TradeSymbol':symbol+"1"},
        {'_id':0,'AddedValueTax':0,'BondDividend':0,'BranchID':0,'Discount':0,'InstrumentCategory':0,'MarketInstrumentISIN':0,'page':0,'Update':0,'dateInt':0,
         'صندوق':0,'DateYrInt':0,'DateMnInt':0,'DateDyInt':0,'TradeSymbolAbs':0,'TradeItemBroker':0,'TradeItemRayanBourse':0,'TradeNumber':0,'TradeSymbol':0,'نام':0}))
    if len(df) == 0:
        return json.dumps({'replay':False,'msg':'گزارشی یافت نشد'})
    df = df[pd.to_numeric(df['NetPrice'], errors='coerce').notnull()]
    df = df.groupby(by=['TradeCode']).apply(Fnc.Apply_Trade_Symbol,symbol = symbol,date=date)
    df['isCompany'] = [x[:4]=='6158' for x in df['TradeCode']]
    df = df.fillna(0)
    dic = {'Volume_Buy':int(df['Volume_Buy'].max()),'Volume_Sell':int(df['Volume_Buy'].max()),'Price_Buy':int(df['Price_Buy'].max()),'Price_Sell':int(df['Price_Sell'].max()),'balance':int(df['balance'].max())}
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})



def desk_broker_turnover(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'replay':False})
    dateTo = data['date'][1]
    dateFrom = data['date'][0]
    df = pd.DataFrame(farasahmDb['deskSabadTurnover'].find({},{'_id':0}))
    df = df.replace(np.inf,0)
    df = df.replace(-np.inf,0)
    df = df.fillna(0)
    mean = {'نسبت اوراق':df['نسبت اوراق'].mean(), 'نسبت صندوق':df['نسبت صندوق'].mean(), 'نسبت سهام':df['نسبت سهام'].mean(), 'نسبت کل':df['نسبت کل'].mean()}
    if dateFrom != None and dateTo != None:
        dateFrom = Fnc.timestumpToJalalInt(dateFrom)
        dateTo = Fnc.timestumpToJalalInt(dateTo)
        df = df[df['date']<=dateTo]
        df = df[df['date']>=dateFrom]
    elif dateFrom != None:
        dateFrom = Fnc.timestumpToJalalInt(dateFrom)
        df = df[df['date']>=dateFrom]
    elif dateTo != None:
        dateTo = Fnc.timestumpToJalalInt(dateTo)
        df = df[df['date']<=dateTo]
    df = df.set_index('date')
    dic = {}
    for c in df.columns:
        if c in ['نسبت کل','نسبت اوراق','نسبت صندوق','نسبت سهام']:
            df[c] = df[c].apply(Fnc.floatTo2decimal)
            df[f'{c} انحراف'] = df[c] - float(Fnc.floatTo2decimal(mean[c]))
            df[f'{c} انحراف'] = df[f'{c} انحراف'].apply(Fnc.floatTo2decimalNormal)
        else:
            df[c] = df[c].apply(Fnc.toBillionRial)
    for c in df.columns:
        dic[c] = float(df[c].max())

    df = df.reset_index()
    df = df.sort_values(by='date',ascending=False)
    

    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df,'dic':dic})


def getinfocode(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False})
    code = data['code']
    dic = farasahmDb['assetsCoustomerBroker'].find_one({'TradeCode':code},{'CustomerTitle':1,'_id':0})
    if dic == None:
        dic = farasahmDb['TradeListBroker'].find_one({'TradeCode':code})
        if dic == None:
            return json.dumps({'reply':False,'msg':'کد معاملاتی یافت نشد'})
        else:
            assets = pd.DataFrame(GetCustomerMomentaryAssets(code))
            if len(assets)>0:
                assets['TradeCode'] = code
                assets['dateInt'] = Fnc.todayIntJalali()
                assets['update'] = datetime.datetime.now()
                assets = assets.to_dict('records')
                farasahmDb['assetsCoustomerBroker'].delete_many({"TradeCode":code,"dateInt":Fnc.todayIntJalali()})
                farasahmDb['assetsCoustomerBroker'].insert_many(assets)
                dic = farasahmDb['assetsCoustomerBroker'].find_one({'TradeCode':code},{'CustomerTitle':1,'_id':0})
                if dic == None:
                    return json.dumps({'reply':False,'msg':'کد معاملاتی یافت نشد'})
            else:
                return json.dumps({'reply':False,'msg':'کد معاملاتی یافت نشد'})
    return json.dumps({'reply':True,'dic':dic['CustomerTitle']})

def codeToInfo(code):
    dic = farasahmDb['assetsCoustomerBroker'].find_one({'TradeCode':code},{'CustomerTitle':1,'_id':0})
    if dic == None:
        dic = farasahmDb['TradeListBroker'].find_one({'TradeCode':code})
        if dic == None:
            return 'نامشخص'
        else:
            assets = pd.DataFrame(GetCustomerMomentaryAssets(code))
            if len(assets)>0:
                assets['TradeCode'] = code
                assets['dateInt'] = Fnc.todayIntJalali()
                assets['update'] = datetime.datetime.now()
                assets = assets.to_dict('records')
                farasahmDb['assetsCoustomerBroker'].delete_many({"TradeCode":code,"dateInt":Fnc.todayIntJalali()})
                farasahmDb['assetsCoustomerBroker'].insert_many(assets)
                dic = farasahmDb['assetsCoustomerBroker'].find_one({'TradeCode':code},{'CustomerTitle':1,'_id':0})
                if dic == None:
                    return 'نامشخص'
            else:
                return 'نامشخص'
    return dic['CustomerTitle']

def desk_sabad_addcodetrader(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False})
    cheack = getinfocode(data)
    if json.loads(cheack)['reply'] == False:
        return cheack
    code = {'name':json.loads(cheack)['dic'],'code':data['code']}
    avalibel = farasahmDb['codeTraderInSabad'].find_one(code)
    if avalibel !=None:
        return json.dumps({'reply':False,'msg':'این کد قبلا موجود بوده است'}) 
    farasahmDb['codeTraderInSabad'].insert_one(code)
    return json.dumps({'reply':True}) 



def codetrader(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['codeTraderInSabad'].find({},{'_id':0}))
    df = df.to_dict("records")
    return json.dumps({'reply':True,'df':df}) 


def delcodetrade(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    code = data['row']['code']
    farasahmDb['codeTraderInSabad'].delete_many({'code':code})
    return json.dumps({'reply':True})


def turnoverpercode(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    date = data['row']['date']
    sabadCode = farasahmDb['codeTraderInSabad'].distinct('code')

    df = pd.DataFrame(farasahmDb['TradeListBroker'].find({"TradeCode": {"$in": sabadCode},"dateInt":date},{'_id':0,'NetPrice':1,'TradeCode':1,'InstrumentCategory':1,'صندوق':1}))
    if len(df)==0:
        return json.dumps({'reply':False,'msg':'داده ای در این تاریخ یافت نشد'})

    df = df.groupby(by=['InstrumentCategory','TradeCode','صندوق']).sum().reset_index()
    df['name'] = [codeToInfo(x) for x in df['TradeCode']]

    labels = list(set(df['name']))
    orag = []
    fund = []
    sahm = []
    for i in labels:
        orghsum = df[df['InstrumentCategory']=='true']
        orghsum = orghsum[orghsum['name']==i]['NetPrice'].sum()
        orag.append(int(orghsum))
        fundsum = df[df['صندوق']==True]
        fundsum = fundsum[fundsum['name']==i]['NetPrice'].sum()
        fund.append(int(fundsum))
        sahamsum = df[df['صندوق']!=True]
        sahamsum = sahamsum[sahamsum['InstrumentCategory']!='true']
        sahamsum = sahamsum[sahamsum['name']==i]['NetPrice'].sum()
        sahm.append(int(sahamsum))

    df = {'labels':labels,'orag':orag,'fund':fund,'sahm':sahm}
    return json.dumps({'reply':True,'df':df})

def perNameMaxdate(group):
    return group[group['date']==group['date'].max()]

def endpriority(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    capitalIns = farasahmDb['capitalIns'].find_one({'_id':ObjectId(data['id'])})
    symbol = capitalIns['symbol']
    dateStr = capitalIns['date']
    dateInt = int(str(dateStr).replace('/',''))
    dfPri = pd.DataFrame(farasahmDb['Priority'].find({'symbol':symbol,'تاریخ':dateStr},{'_id':0,'کد ملی':1,'حق تقدم استفاده شده':1,'date':1})).set_index('کد ملی')
    dfStc = pd.DataFrame(farasahmDb['registerNoBours'].find({"symbol":symbol},{'_id':0}))
    dfStc = dfStc.groupby(by='کد ملی').apply(perNameMaxdate)
    df = dfPri.join(dfStc,how="outer")
    if df[['حق تقدم استفاده شده','نام و نام خانوادگی','کد ملی','تعداد سهام']].isnull().sum().sum() != 0:
        return json.dumps({'reply':False,'msg':'خطا موارد نا مشخص'})
    df['تعداد سهام'] = df['حق تقدم استفاده شده'] + df['تعداد سهام']
    df['date'] = Fnc.todayIntJalali()
    df['rate'] = [int((x / df['تعداد سهام'].sum())*10000)/100 for x in df['تعداد سهام']]
    df['rate'] = [int((x / df['تعداد سهام'].sum())*10000)/100 for x in df['تعداد سهام']]
    df = df.drop(columns=['حق تقدم استفاده شده'])
    df = df.to_dict('records')
    farasahmDb['capitalIns'].update_one({'_id':ObjectId(data['id'])},{'$set':{'enable':False}})
    farasahmDb['Priority'].update_many({'symbol':symbol,'تاریخ':dateStr},{'$set':{'enable':False}})
    farasahmDb['PriorityPay'].update_many({'symbol':symbol},{'$set':{'enable':False}})
    farasahmDb['PriorityTransaction'].update_many({'symbol':symbol},{'$set':{'enable':False}})
    farasahmDb['registerNoBours'].insert_many(df)
    return json.dumps({'reply':True})



def getestelamstocksheet(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol}))
    df = df.groupby(by=['کد ملی']).apply(perNameMaxdate)
    df = df[['نام و نام خانوادگی','کد ملی']]
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})


def desk_todo_addtask(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    dic = data['popup']
    dic['symbol'] = symbol
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    if dic['title'] == "":
        return json.dumps({'reply':False,'msg': "عنوان خالی هست"})
    if dic['discription'] == "":
        return json.dumps({'reply':False,'msg': "توضیحات خالی هست"})
    if dic['deadlineDate'] == "":
        return json.dumps({'reply':False,'msg': "تاریخ سررسید خالی هست"})
    if dic['reminderDate'] == "":
        return json.dumps({'reply':False,'msg': "تاریخ یاداور خالی هست"})
    if dic['reminderDate'] > dic['deadlineDate']:
        return json.dumps({'reply':False,'msg': "تاریخ یادآور باید قبل از سررسید باشد"})
    dic['deadlineDate'] = int(dic['deadlineDate']/1000)
    dic['reminderDate'] = int(dic['reminderDate']/1000)
    farasahmDb["Todo"].insert_one(dic)
    return json.dumps({'reply':True})



def is_valid_day_in_jalali_month(jalali_year, jalali_month, day):

    try:
        return jdatetime.date(jalali_year, jalali_month, day)

    except ValueError:
        nearest_valid_date = None
        current_day = day
        while nearest_valid_date is None:
            current_day -= 1
            try:
                nearest_valid_date = jdatetime.date(jalali_year, jalali_month, current_day)
            except ValueError:
                continue
        return nearest_valid_date

def repetition_Task_Generator(group,toDate):
    repetition = group['repetition'][group.index.min()]
    reminderDate = group['reminderDate'][group.index.min()]
    deadlineDate = group['deadlineDate'][group.index.min()]
    reminder_jalali_date = jdatetime.date.fromgregorian(date=reminderDate.date())
    deadline_jalali_date = jdatetime.date.fromgregorian(date=deadlineDate.date())
    toDate= jdatetime.date.fromgregorian(date= datetime.datetime.fromtimestamp(toDate).date() )
    dateList = []
    if repetition == 'daily':
        step  = datetime.timedelta(days=1)
        while reminder_jalali_date<=toDate:
            lst = [reminder_jalali_date,deadline_jalali_date]
            dateList.append(lst)
            reminder_jalali_date = reminder_jalali_date + step
            deadline_jalali_date = deadline_jalali_date + step
    elif repetition == 'weekly':
        step  = datetime.timedelta(weeks=1)
        while reminder_jalali_date<=toDate:
            lst = [reminder_jalali_date,deadline_jalali_date]
            dateList.append(lst)
            reminder_jalali_date = reminder_jalali_date + step
            deadline_jalali_date = deadline_jalali_date + step

    elif repetition == 'monthly':
        r_year1= reminder_jalali_date.year
        r_month1= reminder_jalali_date.month
        r_day1= reminder_jalali_date.day
        d_year1= deadline_jalali_date.year
        d_month1= deadline_jalali_date.month
        d_day1= deadline_jalali_date.day
        while reminder_jalali_date<=toDate:
            lst = [reminder_jalali_date,deadline_jalali_date]
            dateList.append(lst)
            r_month1 +=1
            if r_month1 > 12 :
                r_month1 = 1
                r_year1 +=1
            d_month1 +=1
            if d_month1 > 12 :
                d_month1 = 1
                d_year1 +=1
            reminder_jalali_date= is_valid_day_in_jalali_month(r_year1, r_month1, r_day1)
            deadline_jalali_date= is_valid_day_in_jalali_month(d_year1, d_month1, d_day1)

    elif repetition == 'quarterly':
        r_year1= reminder_jalali_date.year
        r_month1= reminder_jalali_date.month
        r_day1= reminder_jalali_date.day
        d_year1= deadline_jalali_date.year
        d_month1= deadline_jalali_date.month
        d_day1= deadline_jalali_date.day
        while reminder_jalali_date<=toDate:
            lst = [reminder_jalali_date,deadline_jalali_date]
            dateList.append(lst)
            r_month1 +=3
            if r_month1 > 12 :
                r_month1 = r_month1 -12
                r_year1 +=1
            d_month1 +=3
            if d_month1 > 12 :
                d_month1 = d_month1 -12
                d_year1 +=1
            reminder_jalali_date= is_valid_day_in_jalali_month(r_year1, r_month1, r_day1)
            deadline_jalali_date= is_valid_day_in_jalali_month(d_year1, d_month1, d_day1)


    elif repetition == 'yearly':
        r_year1= reminder_jalali_date.year
        r_month1= reminder_jalali_date.month
        r_day1= reminder_jalali_date.day
        d_year1= deadline_jalali_date.year
        d_month1= deadline_jalali_date.month
        d_day1= deadline_jalali_date.day
        while reminder_jalali_date<=toDate:
            lst = [reminder_jalali_date,deadline_jalali_date]
            dateList.append(lst)
            r_year1 +=1
            d_year1 +=1

            reminder_jalali_date= is_valid_day_in_jalali_month(r_year1, r_month1, r_day1)
            deadline_jalali_date= is_valid_day_in_jalali_month(d_year1, d_month1, d_day1)


    if len(dateList) == 0:
        group['deadlineDate'] = [x.date() for x in group['deadlineDate']]
        group['reminderDate'] = [x.date() for x in group['reminderDate']]
        return group



    dfs = [
        group.assign(reminderDate=new_date[0],deadlineDate=new_date[1])
        for new_date in dateList
    ]
    group = pd.concat(dfs, ignore_index=True).fillna(method='ffill')
    group = group.drop_duplicates()
    group['deadlineDate'] = group['deadlineDate'].apply(Fnc.Jdatetime_to_datetime)
    group['reminderDate'] = group['reminderDate'].apply(Fnc.Jdatetime_to_datetime)
    return group


def filter_last_date(group):
    group =  group[group['date']==group['date'].max()][['act','dateJalali','date']]
    group = group.rename(columns={'date':'date_act'})
    return group



def desk_todo_gettask(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    toDate = int( max(data['DateSelection'])/1000 )
    df = pd.DataFrame(farasahmDb['Todo'].find({'symbol':symbol}))
    if len(df) == 0:
        return json.dumps({'reply':False, 'msg':'هیچ وظیفه ای تعریف نشده'})
    df = df[df['reminderDate']<=toDate]
    df['reminderDate'] = df['reminderDate'].apply(datetime.datetime.fromtimestamp)
    df['deadlineDate'] = df['deadlineDate'].apply(datetime.datetime.fromtimestamp)
    df = df.groupby(['reminderDate','repetition'],as_index=False).apply(repetition_Task_Generator,toDate=toDate)
    if len(df) == 0:
        return json.dumps({'reply':False})
    df = df.reset_index().drop(columns=['level_0','level_1'])

    df = df.sort_values(by='reminderDate',ascending=True)
    df['jalali_reminderDate'] = df['reminderDate'].apply(Fnc.gorgianIntToJalali)
    df['jalali_deadlineDate'] = df['deadlineDate'].apply(Fnc.gorgianIntToJalali)
    df['expier_reminderDate'] = df['reminderDate']<datetime.datetime.now().date()
    df['expier_deadlineDate'] = df['deadlineDate']<datetime.datetime.now().date()
    df['in_list_jalali_reminderDate'] = df.apply(Fnc.replace_values, axis=1)
    
    for i in ['deadlineDate','reminderDate','jalali_reminderDate','jalali_deadlineDate','_id','in_list_jalali_reminderDate']:
        df[i] = df[i].apply(str)

    actDf = pd.DataFrame(farasahmDb['TodoAct'].find({'symbol':symbol},{'_id':0,'symbol':0}))
    if len(actDf) >0:
        actDf = actDf.rename(columns={'task_id':'_id','task_deadlineDate':'deadlineDate'})
        actDf = actDf.set_index(['_id','deadlineDate'])
        df = df.set_index(['_id','deadlineDate'])
        df = df.join(actDf,how='left')
        df = df[df['act']!='done']
        df['to_in_list_jalali_reminderDate'] = df['to_in_list_jalali_reminderDate'].fillna(df['in_list_jalali_reminderDate'])
        df['in_list_jalali_reminderDate'] = df['to_in_list_jalali_reminderDate']

        df = df.drop(columns=['act','to_in_list_jalali_reminderDate','act_date'])
    df = df.reset_index()
    df = df.to_dict('records')

    dic = {}
    dateCuroser = datetime.datetime.now()
    while dateCuroser <= datetime.datetime.fromtimestamp(toDate) + datetime.timedelta(days=1):
        JdateCuroser = Fnc.JalaliDate.to_jalali(dateCuroser)
        JdateCuroser = str(JdateCuroser).replace('-','/')
        dic[JdateCuroser] = []
        dateCuroser = dateCuroser + datetime.timedelta(days=1)
    

    for i in df:
        dateList = str(i['in_list_jalali_reminderDate']).replace('-','/')
        if dateList in dic.keys():
            dic[dateList] = dic[dateList] + [i]

    return json.dumps({'reply':True,'df':dic})


def desk_todo_setact(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    date = datetime.datetime.now()
    task = data['task']
    chack = farasahmDb['TodoAct'].find_one({'symbol':symbol,'task_id':task['_id'],'task_deadlineDate':task['deadlineDate']})
    if chack == None:
        to_in_list = Fnc.JalalistrToGorgia(task['in_list_jalali_reminderDate']) + datetime.timedelta(days=1)
        to_in_list = str(Fnc.JalaliDate.to_jalali(to_in_list))
        farasahmDb['TodoAct'].insert_one({'symbol':symbol,'task_id':task['_id'], 'act':data['act'], 'task_deadlineDate':task['deadlineDate'],'to_in_list_jalali_reminderDate':to_in_list, 'act_date':date})
        return json.dumps({'reply':True})
    else:
        to_in_list = Fnc.JalalistrToGorgia(task['in_list_jalali_reminderDate']) + datetime.timedelta(days=1)
        to_in_list = str(Fnc.JalaliDate.to_jalali(to_in_list))
        farasahmDb['TodoAct'].update_one({'symbol':symbol,'task_id':task['_id'],'task_deadlineDate':task['deadlineDate']},{'$set':{'act':data['act'],'to_in_list_jalali_reminderDate':to_in_list,'act_date':date}})
        return json.dumps({'reply':True})

def merg_df_todo_condrol(group):

    group_parent = group[['_id','title','discription','force','importent','repetition','person','jalali_reminderDate','jalali_deadlineDate','expier_reminderDate','expier_deadlineDate','act']]
    group_child = group[['_id','title','discription','force','importent','repetition','person','jalali_reminderDate','jalali_deadlineDate','expier_reminderDate','expier_deadlineDate','act']]
    group_child = group_child.fillna('')
    group_child = group_child.to_dict('records')
    group_parent = group_parent.sort_values(by='jalali_reminderDate')
    group_parent = group_parent.drop_duplicates(keep='last')
    group_parent['_children'] = [group_child]
    return group_parent

def desk_todo_getcontrol(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['Todo'].find({'symbol':symbol}))

    if   len( df) == 0:
        return json.dumps({'reply':True,'df':list()})
    
    toDate = time.time()

    df['reminderDate'] = df['reminderDate'].apply(datetime.datetime.fromtimestamp)
    df['deadlineDate'] = df['deadlineDate'].apply(datetime.datetime.fromtimestamp)
    df = df.groupby(['reminderDate','repetition'],as_index=False).apply(repetition_Task_Generator,toDate=toDate)

    
    if len(df) == 0:
        return json.dumps({'reply':False})
    df = df.reset_index().drop(columns=['level_0','level_1'])
    df['jalali_reminderDate'] = df['reminderDate'].apply(Fnc.gorgianIntToJalali)
    df['jalali_deadlineDate'] = df['deadlineDate'].apply(Fnc.gorgianIntToJalali)
    df['expier_reminderDate'] = df['reminderDate']<datetime.datetime.now().date()
    df['expier_deadlineDate'] = df['deadlineDate']<datetime.datetime.now().date()
    df['in_list_jalali_reminderDate'] = df.apply(Fnc.replace_values, axis=1)
    for i in ['deadlineDate','reminderDate','jalali_reminderDate','jalali_deadlineDate','_id','in_list_jalali_reminderDate']:
        df[i] = df[i].apply(str)
    
    actDf = pd.DataFrame(farasahmDb['TodoAct'].find({'symbol':symbol},{'_id':0,'symbol':0}))
    if len(actDf)>0:
        actDf = actDf.rename(columns={'task_id':'_id','task_deadlineDate':'deadlineDate'})
        actDf = actDf.set_index(['_id','deadlineDate'])
        df = df.set_index(['_id','deadlineDate'])
        df = df.join(actDf,how='left')
        df = df.drop(columns=['to_in_list_jalali_reminderDate','act_date'])
        df = df.reset_index()
    else:
        df['act'] = ''
    df = df.groupby(by='_id').apply(merg_df_todo_condrol)
    df = df.fillna('')
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})



def desk_todo_deltask(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    idd= ObjectId(data['idTask'])
    del_todo= farasahmDb['Todo'].delete_many({'_id': idd})
    del_todoact= farasahmDb['TodoAct'].delete_many({'task_id': data['idTask']})
    return json.dumps({'reply':True,'msg':"ok"})


def smsgroup(data):
    access = data['access'][0]
    _id= ObjectId(access)
    symbol = data['access'][1]
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    dic = data['data']
    dic['send'] = 'در انتظار'
    dic['count'] = len(data['data']['selectData'])
    dic['count_send'] = 0
    dic['count_deliver'] = 0
    dic['symbol'] = symbol
    farasahmDb['smsGroup'].insert_one(dic)
    return json.dumps({'replay':True})

def smsgroupreport(data):
    access = data['access'][0]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    symbol = data['access'][1]
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['smsGroup'].find({'symbol':symbol}))
    df = df.rename(columns={'selectData':'children'})
    df['_id'] = df['_id'].apply(str)
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})

def getassetfund(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    bank = pd.DataFrame(farasahmDb['bankBalance'].find({'symbol':symbol},{'_id':0}))
    asset = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbol},{'_id':0}))
    asset = asset[asset['date']==asset['date'].max()]
    asset = asset[['MarketInstrumentTitle','VolumeInPrice','type']]
    asset = asset.rename(columns={'MarketInstrumentTitle':'name','VolumeInPrice':'value'})
    asset['num'] = ''
    if len(bank)>0:
        bank = bank.rename(columns={'balance':'value'})
        bank['type'] = 'سپرده بانکی'
        bank = bank[['name','value','type','num']]
        df = pd.concat([asset,bank])
    else:
        df = asset
    
    df = df.fillna('')
    
    df['type'] = df['type'].replace('saham','سهام').replace('non-gov','اوراق شرکتی').replace('gov','اوراق دولتی')
    df['value'] = df['value'].apply(int)
    df['rate'] = df['value'] / df['value'].sum()
    df['warning'] = ''


    x = symbols('x')


    dff = []
    #saham
    df_saham = df[df['type']=='سهام']
    if len(df_saham)>0:
        value = int(df_saham['value'].sum())
        rate = value/df['value'].sum()
        warning = ''
        if rate>0.1:
            warning = 'مجموع سهام بیش از 10 % است'
        df_saham['rate'] = df_saham['rate'].apply(Fnc.StndRt)
        df_saham = df_saham.to_dict('records')
        for i in range(0,len(df_saham)):
            if df_saham[i]['rate']>3:
                df_saham[i]['warning'] = 'این سهم بیش از 3 % است'
                df_saham[i]['max'] = 0
            equation = Eq((df_saham[i]['value'] + x) / (df['value'].sum() + x), 0.03)
            df_saham[i]['max'] = int(solve(equation, x)[0])
            df_saham[i]['min'] = 0

        equation = Eq((value + x) / (df['value'].sum() + x), 0.1)
        dff.append({'type':'سهام', 'value':value, 'rate':Fnc.StndRt(rate), 'warning':warning, '_children':df_saham,'max':int(solve(equation, x)[0]),'min':0})
    else:
        dff.append({'type':'سهام', 'value':0, 'rate':0, 'warning':'', '_children':[],'max':int(df['value'].sum()*0.1), 'min':0})
    
    #oragh
    df_oragh = pd.concat([df[df['type']=='اوراق دولتی'],df[df['type']=='اوراق شرکتی']])
    if len(df_oragh)>0:
        value = int(df_oragh['value'].sum())
        rate = value/df['value'].sum()
        warning = ''
        if rate<0.4:
            warning = 'مجموع کمتر از 40 % است'

        equation = Eq((value + x) / (df['value'].sum() + x), 0.4)
        EqMin = int(solve(equation, x)[0])

        dic = {'type':'اوراق', 'value':value, 'rate':Fnc.StndRt(rate), 'warning':warning, '_children':[],'max':0,'min':EqMin}


        df_dolati = df_oragh[df_oragh['type']=='اوراق دولتی']
        if len(df_dolati)>0:
            Value_dolati = int(df_dolati['value'].sum())
            rate_dolati = Value_dolati / df['value'].sum()
            warning_dolati =''
            if rate_dolati>0.3:
                warning_dolati = 'اوراق دولتی بیشتر از 30 % است'
            elif rate_dolati<0.25:
                warning_dolati = 'اوراق دولتی کمتر از 25 % است'
                
            equationMax = Eq((Value_dolati + x) / (df['value'].sum() + x), 0.3)
            eqMax = int(solve(equationMax, x)[0])
            equationMin = Eq((Value_dolati + x) / (df['value'].sum() + x), 0.25)
            eqMin = int(solve(equationMin, x)[0])

            df_dolati['rate'] = df_dolati['rate'].apply(Fnc.StndRt)
            df_dolati['max'] = 0
            df_dolati['min'] = 0
            dic_dolati = {'type':'دولتی', 'value':Value_dolati, 'rate':Fnc.StndRt(rate_dolati), 'warning':warning_dolati, '_children':df_dolati.to_dict('records'),'max':eqMax,'min':eqMin}
        else:
            equationMax = Eq((Value_dolati + x) / (df['value'].sum() + x), 0.3)
            eqMax = int(solve(equationMax, x)[0])
            dic_dolati = {'type':'دولتی', 'value':0, 'rate':0, 'warning':'اوراق دولتی کمتر از 25 % است', '_children':[],'max':eqMax,'min':0}
        df_sherkat = df_oragh[df_oragh['type']=='اوراق شرکتی']
        if len(df_sherkat)>0:
            value_sherkat = int(df_sherkat['value'].sum())
            rate_sherkat = value_sherkat / df['value'].sum()
            warning_sherkat = ''
            df_sherkat['rate'] = df_sherkat['rate'].apply(Fnc.StndRt)
            df_sherkat['max'] = 0
            df_sherkat['min'] = 0
            dic_sherkat = {'type':'شرکتی', 'value':value_sherkat, 'rate':Fnc.StndRt(rate_sherkat), 'warning':warning_sherkat, '_children':df_sherkat.to_dict('records'),'max':0,'min':0}
        else:
            dic_sherkat = {'type':'شرکتی', 'value':0, 'rate':0, 'warning':'', '_children':[],'max':0,'min':0}
        dic['_children'] = [dic_dolati,dic_sherkat]
        dff.append(dic)
    else:
        dff.append({'type':'اوراق', 'value':0, 'rate':0, 'warning':'مجموع کمتر از 40 % است', '_children':[],'min':EqMin,'max':0})

    df_bank = df[df['type']=='سپرده بانکی']
    if len(df_bank)>0:
        value = int(df_bank['value'].sum())
        rate = value / df['value'].sum()
        warning = ''
        if rate > 0.4:
            warning = 'مجموع سپرده های بانکی بیش از 40 % است'
        equationMax = Eq((value + x) / (df['value'].sum() + x), 0.4)
        eqMax = int(solve(equationMax, x)[0])
        dic = {'type':'سپرده بانکی', 'value':value, 'rate':Fnc.StndRt(rate), 'warning':warning, '_children':[],'max':eqMax,'min':0}

        for i in list(set(df_bank['name'])):
            value_i = int(df_bank[df_bank['name']==i]['value'].sum())
            rate_i = value_i / df['value'].sum()
            warning = ''
            equationMax = Eq((value_i + x) / (df['value'].sum() + x), 0.133)
            eqMax = int(solve(equationMax, x)[0])
            if rate_i>0.133:
                warning = 'سپرده بانکی در این بانک بیش از 13.3 % است'
            df_i = df_bank[df_bank['name']==i]
            df_i['rate'] = df_i['rate'].apply(Fnc.StndRt)
            df_i['max'] = 0
            df_i['min'] = 0
            dic_i = [{'name':i,'type':'سپرده بانکی', 'value':value_i, 'rate':Fnc.StndRt(rate_i), 'warning':warning, '_children':df_i.to_dict('records'),'max':eqMax,'min':0}]
            dic['_children'] = dic['_children'] + dic_i
    else:
        dic = {'type':'سپرده بانکی', 'value':0, 'rate':0, 'warning':'', '_children':[]}
    dff.append(dic)
    return json.dumps({'reply':True,'df':dff})

def getoraghytm(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['oraghYTM'].find({},{'_id':0,'بازده ساده':0}))
    lastUpdate = df['update'].max()
    df = df.fillna(0)
    per1Month = Fnc.JalaliIntToGorgia(lastUpdate) - datetime.timedelta(days=30)
    per1Month = int(str(Fnc.gorgianIntToJalali(per1Month)).replace('-',''))
    df = df[df['update']==lastUpdate]
    df['owner'] = df['نماد'].apply(Fnc.setTypeOraghBySymbol)
    df['owner'] = df['owner'].replace('gov','دولتی').replace('non-gov','شرکتی')
    tse = pd.DataFrame(farasahmDb['tse'].find({'dataInt':{'$gt':int(per1Month)}},{'نماد':1,'حجم':1,'_id':0,'dataInt':1}))
    tse = tse.groupby(by=['نماد']).apply(Fnc.tseGrpByVol)
    tse = tse.drop(columns=['dataInt','نماد'])
    tse = tse.reset_index()
    tse = tse.drop(columns=['level_1'])
    tse = tse.set_index('نماد')
    df = df.set_index('نماد')
    df = df.join(tse,how='left')
    df = df.reset_index()
    df = df.fillna(0)
    df = df.rename(columns={'حجم':'vol'})
    df['vol'] = df['vol'].apply(int)
    df['mean'] = df['mean'].apply(int)
    df['count'] = df['count'].apply(int)
    df['LastDay'] = df['تاریخ سررسید'].apply(Fnc.jalaliStrDifToday)
    df = df.fillna(0)
    dic = {'YTM':int(df['YTM'].max()),'LastDay':int(df['LastDay'].max()),'count':int(df['count'].max()),'mean':int(df['mean'].max()),'vol':int(df['vol'].max())}
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df, 'dic':dic})

def getpriceforward(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    afterday = int(data['afterDay'])
    beforDay = int(data['beforDay'])
    date_start = Fnc.timestumpToJalalInt(data['date'])
    target_rate = int(data['target'])/100
    effect_befor = int(data['befor'])/100
    effect_after = int(data['after'])/100
    df = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbol},{'_id':0,'dateInt':1,'final_price':1}))
    df = df[df['dateInt']>=date_start]
    last_update = df['dateInt'].max()
    df = df.set_index('dateInt')
    
    df['real'] = True
    dateDf = pd.DataFrame(Fnc.calnder())
    dateDf['dateInt'] = [int(x.replace('-','')) for x in dateDf['ja_date']]
    dateDf = dateDf[dateDf['dateInt']>=date_start].reset_index()
    dateDf = dateDf[dateDf.index<365]
    dateDf = dateDf.set_index('dateInt')
    df = df.join(dateDf,how='outer').sort_index().reset_index()[['dateInt','final_price','week','workday','real']]
    df['real'] = df['real'].fillna(False)
    df['real'] = df['final_price'] * df['real']
    df['real'] = df['real'].fillna(method='ffill') 

    start_price = df[df['dateInt']==df['dateInt'].min()].to_dict('records')[0]['final_price']
    df['final_price'] = start_price
    normal_grow = (1 + target_rate) ** (1/365)
    df['normal_grow'] = normal_grow
    
    


    df['befor_diff'] = 1
    df['after_diff'] = 1
    
    df = df.set_index('dateInt')

    for i in df.index:
        normal_grow = df['normal_grow'][i]
        workday = df['workday'][i]
        if workday:
            continue
        df['normal_grow'][i] = 0

        befor_diff = (normal_grow ** effect_befor) ** (1/beforDay)
        after_diff = (normal_grow ** effect_after) ** (1/afterday)
        
        #befor
        dff = df[df['workday'] == True]
        dff = dff[dff.index<i].reset_index()
        if len(dff)>0:
            dff = dff[dff.index>dff.index.max()-beforDay]
            if len(dff)>0:
                dff = dff.reset_index()
                dff['befor_diff'] = befor_diff
                dff = dff[['dateInt','befor_diff']].to_dict('records')
                for b in dff:
                    df['befor_diff'][b['dateInt']] = max(1,df['befor_diff'][b['dateInt']]) * b['befor_diff']
        #after
        dff = df[df['workday'] == True]
        dff = dff[dff.index>i].reset_index()
        if len(dff)>0:
            dff = dff[dff.index<dff.index.min()+afterday]
            if len(dff)>0:
                dff = dff.reset_index()
                dff['after_diff'] = after_diff
                dff = dff[['dateInt','after_diff']].to_dict('records')
                for b in dff:
                    df['after_diff'][b['dateInt']] = max(1,df['after_diff'][b['dateInt']]) * b['after_diff']
            
    
    df['befor_diff'] = df['befor_diff'].replace(0,1)
    df['after_diff'] = df['after_diff'].replace(0,1)
    df['normal_grow'] = df['normal_grow'].replace(0,1)

    df['diff_addon'] = df['befor_diff'] * df['after_diff'] * df['normal_grow']
    df['diff_addon_cum'] = df['diff_addon'].cumprod()
    df['final_price'] = df['diff_addon_cum'] * start_price
    df['final_price'] = df['final_price'] * df['workday']
    df['final_price'] = df['final_price'].replace(0,np.nan)
    df['final_price'] = df['final_price'].fillna(method='ffill')
    df['final_price'] = df['final_price'].apply(round,0)
    df['rate'] = df['final_price'] / df['final_price'].shift(1)
    df['rate'] = df['rate'].fillna(1)
    df['rate'] = (df['rate'] - 1) *1000000000
    df['rate'] = df['rate'].apply(int)/10000000
    df = df.reset_index()
    df['real_'] = df['dateInt'].apply(lambda x: x<last_update)
    df['real'] = df['real'] * df['real_']
    df = df.drop(columns=['real_'])
    df['dateInt'] = df['dateInt'].apply(Fnc.dateIntToSlash)
    df['diff_real'] = df['final_price'] - df['real']
    df['normal_grow'] = (df['normal_grow'] - 1)*100
    df['befor_diff'] = (df['befor_diff'] - 1)*100
    df['after_diff'] = (df['after_diff'] - 1)*100
    df['diff_addon'] = (df['diff_addon'] - 1)*100
    df['diff_addon_cum'] = (df['diff_addon_cum'] - 1)*100
    df['week'] = df['week'].replace(0,'شنبه').replace(1,'یکشنبه').replace(2,'دوشنبه').replace(3,'سه شنبه').replace(6,'جمعه').replace(4,'چهارشنبه').replace(5,'پنج شنبه')
    # df= df.fillna('')
    # print(df)

    df = df.to_dict('records')
    return json.dumps({'reply':True, 'df':df})


# def getpriceforward(data):
#     access = data['access'][0]
#     symbol = data['access'][1]
#     symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
#     _id = ObjectId(access)
#     acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    
#     afterday = int(data['afterDay'])
#     beforDay = int(data['beforDay'])


#     if acc == None:
#         return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
#     date = Fnc.timestumpToJalalInt(data['date'])
#     df = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbol},{'_id':0,'dateInt':1,'final_price':1}))
#     df = df[df['dateInt']>=date]
#     df = df.set_index('dateInt')
#     df['real'] = True
#     dateDf = pd.DataFrame(Fnc.calnder())
#     dateDf['dateInt'] = [int(x.replace('-','')) for x in dateDf['ja_date']]
#     dateDf = dateDf[dateDf['dateInt']>=date].reset_index()
#     dateDf = dateDf[dateDf.index<365]
    
#     dateDf = dateDf.set_index('dateInt')
#     df = df.join(dateDf,how='outer').sort_index().reset_index()[['dateInt','final_price','week','workday','real']]
    
#     df['real'] = df['real'].fillna(False)
#     df['real'] = df['final_price'] * df['real']
    

    
#     df['final_price'] = df['final_price'].fillna(method='ffill')
#     df['workday'] = df['workday'].fillna(False)
    
    
#     df['count_after_holli'] = df.apply(lambda row: Fnc.calculate_future_holidays(row, df), axis=1)
#     df['count_befor_holli'] = df.apply(lambda row: Fnc.calculate_past_holidays(row, df), axis=1)
#     print(df)
    
    
#     dff = df[df['workday']==True]
#     dff = dff.sort_values(by=['dateInt'], ascending=False)
#     dff['count_after_holli'] =dff['count_after_holli'].rolling(beforDay,min_periods=1).sum()
#     dff = dff.sort_values(by=['dateInt'], ascending=True)
#     dff['count_befor_holli'] =dff['count_befor_holli'].rolling(afterday,min_periods=1).sum()
#     dff = dff.set_index('dateInt')
    
    
#     cournt_price = df[df['dateInt']==df['dateInt'].min()].to_dict('records')[0]['final_price']
#     effect = 0
#     grow_rate = ((int(data['target'])/100)+1) ** (1/365)
#     rate_befor = int(data['befor'])/100
#     rate_after = int(data['after'])/100


#     befor_grow_rate = (grow_rate ** rate_befor) ** (1/beforDay)
#     after_grow_rate = (grow_rate ** rate_after) ** (1/afterday)
#     final_price_list = []
#     effect_list = []
    
#     for i in dff.index:
#         final_price_list.append(cournt_price)
#         effect_list.append(effect)
#         count_after_holli_i = dff['count_after_holli'][i]
#         count_befor_holli_i = dff['count_befor_holli'][i]
#         effect_befor = befor_grow_rate ** count_befor_holli_i
#         effect_after = after_grow_rate ** count_after_holli_i
#         effect = (grow_rate ** effect_befor)# ** effect_after
#         cournt_price = cournt_price * effect
#         cournt_price = round(cournt_price,0)
        
#     dff['effect'] = effect_list
#     dff['final_price'] = final_price_list

#     df = df[['dateInt','workday','week']].set_index('dateInt')
#     dff = dff.drop(columns=['workday','week'])
#     df = df.join(dff,how='outer')
#     df.to_excel('df.xlsx')
#     df = df.fillna(method='ffill')
#     df['diff'] = df['final_price'] - df['real']
#     df['rate'] = df['final_price'] / df['final_price'].shift(1)
#     df['rate'] = df['rate'].fillna(1)
#     df['rate'] = [int((x-1)*1000000)/10000 for x in df['rate']]
#     df['week'] = df['week'].apply(int).apply(str)
#     df['week'] = df['week'].replace('0','شنبه').replace('1','یکشنبه').replace('2','دوشنبه').replace('3','سه شنبه').replace('6','جمعه').replace('4','چهارشنبه').replace('5','پنج شنبه')
#     df = df.reset_index()
#     df['dateInt'] = df['dateInt'].apply(Fnc.dateIntToSlash)
#     df = df.to_dict('records')
#     return json.dumps({'reply':True, 'df':df})


# def getpriceforward(data):
#     access = data['access'][0]
#     symbol = data['access'][1]
#     symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
#     _id = ObjectId(access)
#     acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
#     if acc == None:
#         return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
#     date = Fnc.timestumpToJalalInt(data['date'])
#     df = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbol},{'_id':0,'dateInt':1,'final_price':1}))
#     lastUpdate = df['dateInt'].max()
#     df = df[df['dateInt']>=date]
#     df['dateInt'] = [int(x) for x in df['dateInt']]
#     df = df.set_index('dateInt')
#     dateDf = pd.DataFrame(Fnc.calnder())
#     dateDf['dateInt'] = [int(x.replace('-','')) for x in dateDf['ja_date']]
#     dateDf = dateDf[dateDf['dateInt']>=date]
#     dateDf = dateDf.set_index('dateInt')
#     df = df.join(dateDf,how='outer').sort_index().reset_index()
#     df['final_price'] = df['final_price'].fillna(method='ffill')
#     afterday = int(data['afterDay'])
#     beforDay = int(data['beforDay'])
#     df['future_holidays'] = df.apply(lambda row: Fnc.calculate_future_holidays(row, df), axis=1)
#     df['past_holidays'] = df.apply(lambda row: Fnc.calculate_past_holidays(row, df), axis=1)
#     df['real'] = df['dateInt']<=lastUpdate
#     grow_rate = ((int(data['target'])/100)+1) ** (1/365)
#     befor_grow_rate = (((int(data['befor'])/100)/afterday) * (grow_rate - 1)) + 1
#     after_grow_rate = (((int(data['after'])/100)/afterday) * (grow_rate - 1)) + 1

#     df['real'] = df['final_price'] * df['real']
#     df['final_price'] = df[df['dateInt']==df['dateInt'].min()].to_dict('records')[0]['final_price']
#     dfZero = df[df.index==0]
#     dfZero = dfZero[['ja_date','week','workday','real']]
#     df = df[df.index>0]
#     df['grow_rate'] = grow_rate #** df.index
    
#     dff = df[df['workday']==True]
#     df = df.drop(columns=['future_holidays','past_holidays']).set_index('dateInt')
#     dff = dff[['future_holidays','past_holidays','dateInt']]
    
    
#     dff = dff.sort_values(by=['dateInt'], ascending=False)
#     dff['future_holidays'] =dff['future_holidays'].rolling(beforDay,min_periods=1).sum()
#     dff = dff.sort_values(by=['dateInt'], ascending=True)
#     dff['past_holidays'] =dff['past_holidays'].rolling(afterday,min_periods=1).sum()
#     dff = dff.set_index('dateInt')
#     df = df.join(dff)
#     df = df.fillna(0).reset_index()
#     df['grow_holiday_fut'] =  befor_grow_rate ** df['future_holidays']
#     df['grow_holiday_pas'] =  after_grow_rate ** df['past_holidays']
#     df['grow_Fin'] = (df['grow_rate'] * df['workday']) * df['grow_holiday_fut'] * df['grow_holiday_pas']
#     df['grow_Fin'] = df['grow_Fin'].replace(0,np.NaN)
#     df['final_price'] = df['final_price'].fillna(method='ffill')
#     df['grow_Fin'] = df['grow_Fin'].fillna(1)
#     df['grow_Fin'] = df['grow_Fin'].cumprod()
#     df['fut_price'] = df['final_price'] * df['grow_Fin']
#     df['fut_price'] = df['fut_price'].apply(round)
#     df = df[df.index<=365]
#     df = df[['ja_date','week','workday','fut_price','real']]
#     dfZero['fut_price'] = dfZero['real']
#     df = pd.concat([dfZero,df])
#     df['diff'] = df['fut_price'] - df['real']
#     df['diff'] = df['diff'] * ((df['real']>0)*1)
#     df['diff'] = df['diff'].cumsum()
#     df['week'] = df['week'].replace(0,'شنبه').replace(1,'یکشنبه').replace(2,'دوشنبه').replace(3,'سه شنبه').replace(4,'چهارشنبه').replace(5,'پنج شنبه').replace(6,'جمعه')
#     df['workday'] = df['workday'].replace(True,'کاری').replace(False,'تعطیل')
#     df['Chng_price'] = ((df['fut_price'] / df['fut_price'].shift(1)) - 1) * 100000
#     df['Chng_price'] = df['Chng_price'].fillna(0)
#     df['Chng_price'] = df['Chng_price'].apply(int)/1000
#     df = df.to_dict('records')
#     return json.dumps({'reply':True, 'df':df})

def addcompany(access, key, name, idTax, idNum, address, call, postcode):
    accesss = str(access).split(',')
    access = accesss[0]
    symbol = accesss[1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    key = Binary(key.read()).decode()
    dic = {'key':key, 'name':name, 'idTax':idTax, 'idNum':idNum,'date':datetime.datetime.now(), 'symbol':symbol, 'address':address, 'call':call, 'postcode':postcode}
    if farasahmDb['companyMoadian'].find_one({'name':name}) != None:
        return json.dumps({'reply':False,'msg':'نام شرکت تکراری است'})
    if farasahmDb['companyMoadian'].find_one({'idTax':idTax}) != None:
        return json.dumps({'reply':False,'msg':'شناسه حافظه تکراری است'})
    if farasahmDb['companyMoadian'].find_one({'idNum':idNum}) != None:
        return json.dumps({'reply':False,'msg':'شناسه اقتصادی/ملی تکراری است'})
    farasahmDb['companyMoadian'].insert_one(dic)
    return json.dumps({'reply':True})

def getcompanymoadian(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['companyMoadian'].find({'symbol':symbol},{'_id':0,'key':0,'symbol':0}))
    if len(df) == 0:
        df['date'] = df['date'].apply(Fnc.gorgianIntToJalaliInt)
        df['key'] = '*'*10
    df['date'] = df['date'].apply(Fnc.dateToIntJalali)
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})

def delcompanymoadian(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    row = data['row']
    farasahmDb['companyMoadian'].delete_many({'idNum':row['idNum'],'name':row['name'],'symbol':symbol})
    return json.dumps({'reply':True})

def getlistcompanymoadian(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['companyMoadian'].find({'symbol':symbol},{'_id':0,'name':1,'idNum':1})).to_dict('records')
    if len(df)==0:
        return json.dumps({'reply':False,'msg':'هیچ شرکتی ثبت نشده'})
    return json.dumps({'reply':True,'df':df})

def saveinvoce(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    invoceData = data['invoceData']
    sellerDic = farasahmDb['companyMoadian'].find_one({'idNum':invoceData['sellerId']})
    if sellerDic == None:
        return json.dumps({'reply':False,'msg':'فروشنده یافت نشد'})
    bodyDf = pd.DataFrame(invoceData['body'])
    bodyDf['sumBeforOff'] = bodyDf['sumBeforOff'].apply(int)
    bodyDf['off'] = bodyDf['off'].apply(int)
    bodyDf['taxRate'] = bodyDf['taxRate'].apply(int)
    bodyDf['cash'] = bodyDf['cash'].apply(int)
    bodyDf['sumAfterOff'] = bodyDf['sumBeforOff'] - bodyDf['off']
    bodyDf['sumTax'] = bodyDf['sumAfterOff'] * (bodyDf['taxRate'] /100)
    bodyDf['sumTax'] = bodyDf['sumTax'].apply(int)
    bodyDf['sumFin'] = bodyDf['sumAfterOff'] + bodyDf['sumTax']
    mmrit = sellerDic['idTax']
    indatim = int(invoceData['createDate'])
    Indati2m = int(invoceData['addDate'])
    if indatim>Indati2m:
        return json.dumps({'reply':False,'msg':'تاریخ صدور نمیتواند قبل از تاریخ فروش باشد'})
    date = datetime.datetime.fromtimestamp(indatim/1000)
    dateJalali = Fnc.gorgianIntToJalali(date)
    inno = Fnc.generatIdInternal()
    taxid = Fnc.generate_tax_id(mmrit,dateJalali,inno)
    if bodyDf['cash'].sum() == 0:
        setm = 2
        tvop = 0
    elif (bodyDf['cash'].sum() + bodyDf['sumTax'].sum()) == bodyDf['sumFin'].sum():
        setm = 1
        tvop = int(bodyDf['sumTax'].sum())
    else:
        setm = 3
        tvop = int(bodyDf['sumTax'].sum())


    header = {
            "taxid": taxid,
            "indatim": indatim,
            "indati2m": Indati2m,
            "inty": int(invoceData['type']),
            "inno" : format(inno, '010X'),
            "irtaxid" : None,
            "inp": int(invoceData['patern']),
            "ins" : 1, # موقع ارسال باید عوض شود
            "tins" : str(sellerDic['idNum']),
            "tinb" : str(invoceData['buyerId']),
            "tob" : int(invoceData['buerType']),
            "bid" : None,
            "sbc" : None,
            "bpc" : None,
            "bbc" : None,
            "ft" : None,
            "bpn" : None,
            "scln" : None,
            "scc" : None,
            "crn" : None,
            "billid" :None,
            "tprdis" : int(bodyDf['sumBeforOff'].sum()),
            "tdis" : int(bodyDf['off'].sum()),
            "tadis" : int(bodyDf['sumAfterOff'].sum()),
            "tvam" : int(bodyDf['sumTax'].sum()),
            "todam" : 0,
            "tbill" : int(bodyDf['sumFin'].sum()),
            "setm" : setm,
            "cap" : int(bodyDf['cash'].sum()),
            "insp" : int(bodyDf['sumFin'].sum() - bodyDf['cash'].sum()) - tvop,
            "tvop" : tvop,
            "tax17" : None, 
        }
    body = []
    bodyDf = bodyDf.to_dict('records')
    for i in bodyDf:
        row ={
            "sstid" : str(i['idProduct']),
            "sstt" : str(i['discription']),
            "am" : int(i['count']),
            "mu": None,
            "fee" : int(i['sumBeforOff'] / int(i['count'])),
            "cfee" : None,
            "cut" : None,
            "exr" : None,
            "prdis" : int(i['sumBeforOff']),
            "dis" : int(i['off']),
            "adis" : int(i['sumAfterOff']),
            "vra" : int(i['taxRate']),
            "vam" : int(i['sumTax']),
            "odt" : None,
            "odr" : None,
            "odam" : None,
            "olt" : None,
            "olr" : None,
            "olam" : None,
            "consfee" : None,
            "spro" : None,
            "bros" : None,
            "tcpbs" : None,
            "cop" : int(i['cash']),
            "vop" : int(i['cash'] * (i['taxRate']/100)),
            "bsrn" : None,
            "tsstam" : int(i['sumFin']),
        }
        body.append(row)
    invoice = {
        'header' : header,
        'body' : body,
        'payments' : []
    }
    buyer = ''#data['buyer']
    #buyer['idcode'] = str(invoceData['buyerId'])
    dic = {'title':invoceData['title'],'date':datetime.datetime.now(),'invoice':invoice,'result':None,'inquiry':None, 'details':{'seler':sellerDic,'buyer':buyer,}}#'text':data['textinv']}}
    farasahmDb['invoiceMoadian'].insert_one(dic)

    return json.dumps({'reply':True})



def getdiffassetamarydash(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    dic = farasahmDb['sandoq'].find_one({'symbol':symbol,'navAmary':{'$gt':0}},sort=[('dateInt', -1)])
    asset = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbol}))
    asset = asset[asset['date']==asset['date'].max()]
    asset = asset['VolumeInPrice'].apply(int).sum()
    bank = pd.DataFrame(farasahmDb['bankBalance'].find({'symbol':symbol}))
    bank['balance'] = bank['balance'].apply(int)
    bank = bank['balance'].sum()
    allRegistered = int(asset) + int(bank)
    allAmary = int(dic['navAmary']) * int(dic['countunit'])
    return json.dumps({'reply':True,'lab':{'ارزش ثبت شده':'ارزش ثبت شده','آماری سایت':'آماری سایت'},'val':{'ارزش ثبت شده':allRegistered,'آماری سایت':allAmary}})


def getdiffnavprc(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    condition = {'dateInt': {'$exists': True},'symbol':symbol}  # شرط لازم برای وجود فیلد dateInt
    period = data['period']
    if period == 'ماه':
        limit = 30
    elif period =='فصل':
        limit = 90
    elif period =='شش ماه':
        limit = 180
    elif period =='سال':
        limit = 365
    elif period =='همه':
        limit = 900
    df = pd.DataFrame(farasahmDb['sandoq'].find(condition, sort=[('dateInt', -1)]).limit(limit))
    df = df[['dateInt','nav','close_price']]
    df['dateInt'] = df['dateInt'].apply(str)
    df = df.set_index(['dateInt'])
    df['dateInt'] = df.index
    df = df.to_dict('dict')
    return json.dumps({'reply':True,'df':df})



def getretrnprice(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbol},{'_id':0,'close_price':1,'dateInt':1}))
    if data['period'] == 'روزانه':
        df['period'] = df['dateInt']
        mult = 1
    elif data['period'] == 'هفتگی':
        df['dateGorgia'] = df['dateInt'].apply(Fnc.JalaliIntToGorgia)
        maxDate = df['dateGorgia'].max()
        df['period'] = (maxDate - df['dateGorgia']).dt.days // 7
        df = df.drop(columns=['dateGorgia'])
        mult = 7
    elif data['period'] == 'ماهانه':
        df['dateGorgia'] = df['dateInt'].apply(Fnc.JalaliIntToGorgia)
        maxDate = df['dateGorgia'].max()
        df['period'] = (maxDate - df['dateGorgia']).dt.days // 30
        df = df.drop(columns=['dateGorgia'])
        mult = 30
    elif data['period'] == 'فصلی':
        df['dateGorgia'] = df['dateInt'].apply(Fnc.JalaliIntToGorgia)
        maxDate = df['dateGorgia'].max()
        df['period'] = (maxDate - df['dateGorgia']).dt.days // 90
        df = df.drop(columns=['dateGorgia'])
        mult = 90
    elif data['period'] == 'شش ماهه':
        df['dateGorgia'] = df['dateInt'].apply(Fnc.JalaliIntToGorgia)
        maxDate = df['dateGorgia'].max()
        df['period'] = (maxDate - df['dateGorgia']).dt.days // 180
        df = df.drop(columns=['dateGorgia'])
        mult = 180

    df = df.groupby(by=['period']).apply(Fnc.retnFixInByPrd)
    df = df.sort_values('dateInt',ascending=False).reset_index()
    try:df = df.drop(columns=['period','level_1'])
    except:pass
    df['aft_price'] = df['close_price'].shift(1)
    
    df['YTM'] = df['aft_price'] / df['close_price']
    try:
        if data['method'] == 'مرکب':
            df['YTM'] = df['YTM'] ** (365/mult)
        elif data['method'] == 'ساده':
            df['YTM'] = (df['YTM']-1) * (365/mult)
            df['YTM'] = df['YTM'] + 1
        else:
            df['YTM'] = df['YTM']
    except:
        df['YTM'] = df['YTM'] ** (365/mult)
    df = df.dropna()
    df['YTM'] = df['YTM'] - 1
    df['YTM'] = df['YTM'] * 10000
    df['YTM'] = df['YTM'].apply(int)/100
    df = df[df.index<30]
    df['date'] = df['dateInt']
    df = df.set_index('dateInt')
    df = df.to_dict('dict')
    return json.dumps({'reply':True,'df':df})


def getrateassetfixincom(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    bank = pd.DataFrame(farasahmDb['bankBalance'].find({'symbol':symbol},{'_id':0}))

    if len(bank)>0:
        bank['balance'] = bank['balance'].apply(int)
        bank = bank['balance'].sum()

    else:
        bank = 0

    asset = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbol},{'_id':0}))
    asset = asset[asset['date']==asset['date'].max()]
    asset = asset[['MarketInstrumentTitle','VolumeInPrice','type']]

    saham = asset[asset['type']=='saham']
    if len(saham)>0:
        saham = saham['VolumeInPrice'].sum()
    else:
        saham = 0
    nogov = asset[asset['type']=='non-gov']
    if len(nogov)>0:
        nogov = nogov['VolumeInPrice'].sum()
    else:
        nogov =0

    gov = asset[asset['type']=='gov']
    if len(gov)>0:
        gov = gov['VolumeInPrice'].sum()
    else:
        gov =0

    lst = [bank, saham, nogov, gov]
    lst = [round((int(x)/sum(lst))*100,1) for x in lst]
    #lst = {'بانک':int(bank), 'سهام':int(saham), 'اوراق شرکتی':int(nogov), 'اوراق دولتی':int(gov)}
    lab = {'بانک':'بانک','سهام':'سهام','اوراق شرکتی':'اوراق شرکتی','اوراق دولتی':'اوراق دولتی'}
    lab = ['بانک','سهام','اوراق شرکتی','اوراق دولتی']
    return json.dumps({'reply':True,'lab':lab, 'lst':lst})


def getpotentialcoustomer(data):

    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})


    df = pd.DataFrame(farasahmDb['assetCoustomerOwnerFix'].find({},{'_id':0,'CustomerTitle':1,'MarketInstrumentTitle':1,'Symbol':1,'Volume':1,'VolumeInPrice':1,'dateInt':1,'TradeCode':1}))
    df = df.drop_duplicates()

    df['target'] = df['Symbol'].isin(symbolTarget)
    df = df[df['target']==True]
    df['VolumeInPrice'] = df['VolumeInPrice'].apply(int)
    df = df[df['VolumeInPrice']>=500000000]

    df = df.groupby(by='TradeCode').apply(Fnc.grouppotential)


    df = df.reset_index().drop(columns=['TradeCode','level_1'])
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})


def getonwerfix(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['assetsCoustomerBroker'].find({'Symbol':symbol+'1'},{'_id':0,'CustomerTitle':1,'Volume':1,'dateInt':1,'TradeCode':1}))
    df = df.groupby('TradeCode').apply(Fnc.groupTradeCodeinLastDate)
    df['Volume'] = df['Volume'].apply(int)
    df = df.sort_values(by='Volume',ascending=False)
    df = df.drop(columns=['TradeCode','dateInt']).reset_index()
    df = df.drop(columns=['TradeCode','level_1'])
    df = df[df['CustomerTitle']!='ETF کد رزرو صندوقهای سرمایه گذاری قابل معامله']
    df = df[df.index<=20]
    df.index = df['CustomerTitle']
    df = df.to_dict('dict')
    return json.dumps({'reply':True,'df':df})

def getrankfixin(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['sandoq'].find({'type':'sabet'},{'_id':0}))
    df = df.groupby(by='symbol').apply(Fnc.fund_compare_clu_ccp)
    df = df[['ret_ytm_7','ret_ytm_14','ret_ytm_30','ret_ytm_90','ret_ytm_180','ret_ytm_365','ret_ytm_730']]
    for i in df.columns:
        df[f'{i}_count'] = len(df[df[i]>0])
        df[i] = df[i].rank(ascending=False)
    df = df.reset_index()
    df = df[df['symbol']==symbol].drop(columns=['level_1'])
    df = df.to_dict('records')[0]
    return json.dumps({'reply':True,'df':df})
    

#ناقض
def calcincass(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    bank = pd.DataFrame(farasahmDb['bankBalance'].find({'symbol':symbol},{'_id':0}))
    asset = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbol},{'_id':0}))
    asset = asset[asset['date']==asset['date'].max()]
    asset = asset[['MarketInstrumentTitle','VolumeInPrice','type']]
    asset = asset.rename(columns={'MarketInstrumentTitle':'name','VolumeInPrice':'value'})
    asset['num'] = ''
    if len(bank)>0:
        bank = bank.rename(columns={'balance':'value'})
        bank['type'] = 'bank'
        bank = bank[['name','value','type']]
        df = pd.concat([asset,bank])
    data = data['inp']
    df['value'] = df['value'].apply(int)
    df = df.groupby('type').sum()
    df['rate_befor'] = df['value'] / df['value'].sum()
    df['return'] = 0
    increase = int(data['increase'])*1000
    if 'bank' in df.index:
        df['return']['bank'] = float(data['bank']) /100
    if 'gov' in df.index:
        df['return']['gov'] = float(data['gov']) /100
    if 'non-gov' in df.index:
        df['return']['non-gov'] = float(data['nogov']) /100
    if 'saham' in df.index:
        df['return']['saham'] = float(data['saham']) /100

    olaviat = list(df.sort_values(by=['return'],ascending=False).index)
    if data['type'] == 'محافظه کار':
        if 'saham' in olaviat:
            olaviat.remove('saham')
            olaviat.append('saham')
    
    df['approve'] = 0
    increaseCro = increase
    for i in olaviat:
        if increaseCro<=0:
            break
        if i == 'bank':
            value = df['value'][i]
            limit = int((df['value'].sum() + increase) * 0.4)
            limit = limit - value
            if limit >= increaseCro:
                balance  = increaseCro
            else:
                balance = limit
            if balance > 0:
                df['approve'][i] = balance
                increaseCro = increaseCro - balance

        elif i == 'saham':
            value = df['value'][i]
            limit = int((df['value'].sum() + increase) * 0.1)
            limit = limit - value

            if limit >= increaseCro:
                balance  = increaseCro
            else:
                balance = limit
            increaseCro = increaseCro - balance
            if balance > 0:
                df['approve'][i] = balance
        
        elif i == 'gov':
            value = df['value'][i]
            limit = int((df['value'].sum() + increase) * 0.35)
            limit = limit - value
            if limit >= increaseCro:
                balance  = increaseCro
            else:
                balance = limit
            increaseCro = increaseCro - balance
            if balance > 0:
                df['approve'][i] = balance

                
        else:
            balance  = increaseCro
            increaseCro = 0
            if balance > 0:
                df['approve'][i] = balance

    df['value_after'] = df['approve'] + df['value']
    df['rate_after'] = df['value_after'] / df['value_after'].sum()
    df = df.reset_index()
    df['name'] = df['type'].replace('bank','بانک').replace('saham','سهم').replace('gov','اوراق دولتی').replace('non-gov','اوراق شرکتی')
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})


def getinvoce(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['invoiceMoadian'].find({},{'invoice':0,'details':0}))
    df['date'] = df['date'].apply(Fnc.gorgianIntToJalali)
    df['date'] = df['date'].apply(str)
    df['_id'] = df['_id'].apply(str)
    df['uid'] = df['result'].apply(Fnc.resultToUid)
    df['referenceNumber'] = df['result'].apply(Fnc.resultToReferenceNumber)
    df['status'] = df['inquiry'].apply(Fnc.resultToStatus)
    df['error'] = df['inquiry'].apply(Fnc.resultToError)
    df = df.to_dict('records')
    return json.dumps({'reply':True, 'df':df})

    
def sendinvoce(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _idinv = ObjectId(data['id'])
    invoce = farasahmDb['invoiceMoadian'].find_one({'_id':_idinv})['invoice']
    seller = farasahmDb['companyMoadian'].find_one({'idNum':invoce['header']['tins']})
    key = seller['key']
    memoryId = seller['idTax']
    moadian = Moadian(memoryId, key)
    result = moadian.send_invoice(invoce)
    result = result['result'][0]
    farasahmDb['invoiceMoadian'].update_one({'_id':_idinv},{'$set':{'result':result,'inquiry':None}})
    return json.dumps({'reply':True})

def ebtalinvoce(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _idinv = ObjectId(data['id'])
    ref = farasahmDb['invoiceMoadian'].find_one({'_id':_idinv},{'_id':0})
    tins = ref['invoice']['header']['tins']
    sellerDic = farasahmDb['companyMoadian'].find_one({'idNum':tins})
    if sellerDic == None:
        return json.dumps({'reply':False,'msg':'فروشنده یافت نشد'})
    indatim = int(datetime.datetime.now().timestamp() *1000)
    date = datetime.datetime.fromtimestamp(indatim/1000)
    dateJalali = Fnc.gorgianIntToJalali(date)
    inno = Fnc.generatIdInternal()
    mmrit = sellerDic['idTax']
    ref['title'] = ref['title'] + '_ابطال'
    ref['invoice']['header']['ins'] = 3
    ref['invoice']['header']['indatim'] = indatim
    ref['invoice']['header']['indati2m'] = indatim + 1000
    ref['invoice']['header']['irtaxid'] = ref['invoice']['header']['taxid']
    ref['invoice']['header']['taxid'] = Fnc.generate_tax_id(mmrit,dateJalali,inno)
    ref['result'] = None
    ref['inquiry'] = None
    farasahmDb['invoiceMoadian'].insert_one(ref)
    return json.dumps({'reply':True})

def inquiryinvoce(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _idinv = ObjectId(data['id'])
    invoce = farasahmDb['invoiceMoadian'].find_one({'_id':_idinv})
    try:
        if 'referenceNumber' not in invoce['result']:
            return json.dumps({'reply':False,'msg':'ابتدا باید ارسال شود'})
    except:
            return json.dumps({'reply':False,'msg':'ابتدا باید ارسال شود'})
    seller = farasahmDb['companyMoadian'].find_one({'idNum':invoce['invoice']['header']['tins']})
    key = seller['key']
    memoryId = seller['idTax']
    moadian = Moadian(memoryId, key)
    referenceNumber = invoce['result']['referenceNumber']
    inquiry = moadian.inquiry_by_reference_number(referenceNumber)['result']['data'][0]
    farasahmDb['invoiceMoadian'].update_one({'_id':_idinv},{'$set':{'inquiry':inquiry}})

    return json.dumps({'reply':True})


def getretassfix(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    bank = pd.DataFrame(farasahmDb['bankBalance'].find({'symbol':symbol},{'_id':0,'balance':1,'rate':1}))
    bank['rate'] = bank['rate'].apply(float)
    bank['balance'] = bank['balance'].apply(float)
    bank['rate'] = bank['rate'] / 100
    bank = int(((bank['rate'] * bank['balance']).sum() / bank['balance'].sum())*1000)/10

    df = pd.DataFrame(farasahmDb['oraghYTM'].find({},{'_id':0,'بازده ساده':0}))
    lastUpdate = df['update'].max()
    df = df.fillna(0)
    per1Month = Fnc.JalaliIntToGorgia(lastUpdate) - datetime.timedelta(days=30)
    per1Month = int(str(Fnc.gorgianIntToJalali(per1Month)).replace('-',''))
    df = df[df['update']==lastUpdate]
    df['owner'] = df['نماد'].apply(Fnc.setTypeOraghBySymbol)
    tse = pd.DataFrame(farasahmDb['tse'].find({'dataInt':{'$gt':int(per1Month)}},{'نماد':1,'حجم':1,'_id':0,'dataInt':1}))
    tse = tse.groupby(by=['نماد']).apply(Fnc.tseGrpByVol)
    tse = tse.drop(columns=['dataInt','نماد'])
    tse = tse.reset_index()
    tse = tse.drop(columns=['level_1'])
    tse = tse.set_index('نماد')
    df = df.set_index('نماد')
    df = df.join(tse,how='left')
    df = df[['YTM', 'owner', 'mean']].dropna()

    gov = df[df['owner']=='gov']
    gov = gov.sort_values(by='mean', ascending=False).head(10)['YTM'].mean()
    gov = round(gov,1)

    nongov = df[df['owner']=='non-gov']
    nongov = nongov.sort_values(by='mean', ascending=False).head(10)['YTM'].mean()
    nongov = round(nongov,1)


    dic = {'bank':bank,'gov':gov,'nongov':nongov}

    
    return json.dumps({'reply':True,'dic':dic})






def getratretasst(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    dff = pd.DataFrame(farasahmDb['ReturnRateAssetFund'].find({'symbol':symbol}))
    df = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbol},{'_id':0,'MarketInstrumentTitle':1,'type':1,'date':1}))
    if len(df)==0:
        return json.dumps({'reply':False, 'msg':'هیچ دارایی یافت نشد'})
    df = df[df['date'] == df['date'].max()]
    df['type'] = df['type'].replace("non-gov","شرکتی").replace("gov","دولتی").replace("saham","سهم")
    if len(dff) == 0:
        df['rate'] = 0
        df = df[['MarketInstrumentTitle', 'type', 'rate']]
        df = df.to_dict('records')
        return json.dumps({'reply':True,'df':df})
    dff = dff[dff['date']==dff['date'].max()]
    dff = dff[['MarketInstrumentTitle','rate']]
    
    df = dff.set_index('MarketInstrumentTitle').join(df.set_index('MarketInstrumentTitle'), how='right')
    
    df = df.fillna(0)
    df = df.reset_index()
    df = df[['MarketInstrumentTitle', 'type', 'rate']]
    print(df)
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})


def saverateretasst(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(data['df'])
    df = df[['MarketInstrumentTitle','rate']]
    try:
        df['rate'] = df['rate'].apply(float)
    except:
        return json.dumps({'reply':False,'msg':'نرخ های باید بصورت عدد وارد شوند'})
    df['symbol'] = symbol
    today = Fnc.todayIntJalali()
    df['date'] = today
    df = df.to_dict('records')
    farasahmDb['ReturnRateAssetFund'].delete_many({'symbol':symbol,'date':today})
    farasahmDb['ReturnRateAssetFund'].insert_many(df)
    return json.dumps({'reply':True})



def getreturnasset(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})


    dff = pd.DataFrame(farasahmDb['ReturnRateAssetFund'].find({'symbol':symbol}))
    df = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbol},{'_id':0,'MarketInstrumentTitle':1,'type':1,'date':1,'VolumeInPrice':1}))
    if len(df)==0:
        return json.dumps({'reply':False, 'msg':'هیچ دارایی یافت نشد'})
    df = df[df['date'] == df['date'].max()]
    df['type'] = df['type'].replace("non-gov","شرکتی").replace("gov","دولتی").replace("saham","سهم")
    if len(dff) == 0:
        df['rate'] = 0
        df = df[['MarketInstrumentTitle', 'type', 'rate','VolumeInPrice']]
    else:
        dff = dff[dff['date']==dff['date'].max()]
        dff = dff[['MarketInstrumentTitle','rate']]
        df = dff.set_index('MarketInstrumentTitle').join(df.set_index('MarketInstrumentTitle'), how='right')
        df = df.fillna(0)
        df = df.reset_index()
        df = df[['MarketInstrumentTitle', 'type', 'rate','VolumeInPrice']]
    
    asset = df
    bank = pd.DataFrame(farasahmDb['bankBalance'].find({'symbol':symbol},{'_id':0,"name":1,'balance':1,'rate':1,'return':1}))
    bank = bank.rename(columns={'name':'MarketInstrumentTitle','balance':'VolumeInPrice'})
    for i in bank.index:
        if bank['return'][i] == 'monthly':
            rateYtm = float(bank['rate'][i])
            rateYtm = rateYtm / 100
            rateYtm = rateYtm / 12
            rateYtm = rateYtm + 1
            rateYtm = rateYtm ** 12
            rateYtm = rateYtm - 1
            bank['rate'][i] = rateYtm
    bank['type'] = 'بانک'
    bank = bank.replace('','1')
    bank = bank[['MarketInstrumentTitle','VolumeInPrice','rate']]
    df = pd.concat([df,bank])
    df['VolumeInPrice'] = df['VolumeInPrice'].apply(int)
    df['rate'] = df['rate'].apply(float)
    df['retn'] = df['VolumeInPrice'] * df['rate']
    retn = df['retn'].sum() / df['VolumeInPrice'].sum()
    df['rate'] = df['rate'] *100
    df['rate'] = df['rate'].apply(int)
    df['rate'] = df['rate'] / 100
    retn = retn *100
    retn = int(retn)/100
    df = df[['MarketInstrumentTitle','VolumeInPrice','rate']]
    df = df.to_dict('records')
    return json.dumps({'reply':True, 'df':df,'retn':retn})



def staticownerincomp(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = farasahmDb['TradeListBroker'].find({'TradeSymbol':symbol+'1'},{'_id':0,'TradeCode':1, 'Volume':1, 'dateInt':1,  "TradeType":1})
    df = pd.DataFrame(df)
    df = df[df['TradeCode']!="61580186248"]
    df = df[['Volume', 'dateInt','TradeType']]
    df['len'] = 1
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0).astype(int)
    df['Volume_Buy'] = df['Volume'] * (df['TradeType'] == 'Buy')
    df['Volume_Sel'] = df['Volume'] * (df['TradeType'] != 'Buy')
    df = df.groupby('dateInt').sum()
    df = df.dropna()
    df['balance'] = df['Volume_Buy'] - df['Volume_Sel']
    df = df.sort_index(ascending=False)
    df['dateInt'] = df.index

    df = df[['balance','dateInt']]
    df = df.to_dict('dict')
    return json.dumps({'reply':True, 'df':df})


def dashpotantial(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['assetCoustomerOwnerFix'].find({},{'_id':0,'CustomerTitle':1,'MarketInstrumentTitle':1,'Symbol':1,'Volume':1,'VolumeInPrice':1,'dateInt':1,'TradeCode':1}))
    df = df[df['TradeCode']!='61580209324']
    df = df.drop_duplicates()
    df = df[df['Symbol']!=symbol+'1']
    df['target'] = df['Symbol'].isin(symbolTarget)
    df = df[df['target']==True]
    df['VolumeInPrice'] = df['VolumeInPrice'].apply(int)
    df = df[df['VolumeInPrice']>=1500000000]
    df = df.groupby(by='TradeCode').apply(Fnc.grouppotentialNoChild)
    df = df.sort_values('VolumeInPrice',ascending=False).reset_index().drop(columns=['TradeCode','level_1'])
    df = df[df['CustomerTitle']!='ETF کد رزرو صندوقهای سرمایه گذاری قابل معامله']
    df = df[df.index<15]
    df = df[['CustomerTitle','VolumeInPrice']]
    df = df.set_index('CustomerTitle')
    df['Customer'] = df.index
    df = df.to_dict('dict')
    return json.dumps({'reply':True,'df':df})

def dashpotantialsymbol(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['assetCoustomerOwnerFix'].find({},{'_id':0,'CustomerTitle':1,'MarketInstrumentTitle':1,'Symbol':1,'Volume':1,'VolumeInPrice':1,'dateInt':1,'TradeCode':1}))
    df = df.drop_duplicates()
    df = df[df['Symbol']!=symbol+'1']
    df['target'] = df['Symbol'].isin(symbolTarget)
    df = df[df['target']==True]
    df['VolumeInPrice'] = df['VolumeInPrice'].apply(int)
    df = df.groupby(by='Symbol').apply(Fnc.grouppotentialSymbol)
    df = df.sort_values('VolumeInPrice',ascending=False)
    df = df.reset_index()
    df = df[df.index<15]
    df = df[['VolumeInPrice','Symbol']]
    df = df.set_index('Symbol')
    df['Symbol'] = df.index
    df = df.to_dict('dict')
    return json.dumps({'reply':True,'df':df})


#سهامداران رسوبی
def getresidual(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    max_date_record = farasahmDb['register'].find_one({'symbol':symbol},sort=[('تاریخ گزارش', pymongo.DESCENDING)])['تاریخ گزارش']
    df = farasahmDb['register'].find({'symbol':symbol,'تاریخ گزارش':max_date_record},{'_id':0, 'کد ملی':1, 'سهام کل':1, 'تاریخ تولد':1, 'نوع سهامدار':1 ,'تاریخ گزارش':1 ,'کد سهامداری':1, 'نام خانوادگی ':1, 'نام':1,  'محل صدور':1, 'نوع':1, 'جنسیت':1 ,'شناسه ملی':1})
    df = pd.DataFrame(df)
    listCode = df['کد سهامداری'].tolist()
    dfTrade = farasahmDb['traders'].find({"کد": {"$in": listCode}, "symbol": symbol},{'_id':0,'کد':1,'تعداد فروش':1,'تعداد خرید':1,'date':1})
    dfTrade = pd.DataFrame(dfTrade)
    if data['type'] == 'sell':
        dfTrade = dfTrade[dfTrade['تعداد فروش']>0]
    if data['type'] == 'buy':
        dfTrade = dfTrade[dfTrade['تعداد خرید']>0]

    dfTrade = dfTrade.sort_values(by='date', ascending=False)
    dfTrade = dfTrade.drop_duplicates(subset='کد', keep='first')

    df = df.set_index('کد سهامداری')
    dfTrade = dfTrade.set_index('کد')
    df = df.join(dfTrade,how='outer')
    df = df.reset_index()
    min_date_value = df['date'].min()
    df['date'] = df['date'].fillna(min_date_value)
    df['date'] = df['date'].apply(int)
    df['lastTradeDay'] = df['date'].apply(Fnc.diffJalaliIntToToday)
    max_last_Trade = df['lastTradeDay'].max()
    target = int(data['target'])

    for i in range(0,max_last_Trade,target):
        condition = (df['lastTradeDay'] >= i) & (df['lastTradeDay'] < i+100)
        df.loc[condition, 'lastTradeDay'] = i
    df['سهام کل'] = df['سهام کل'].apply(int)
    df['rate'] = df['سهام کل'] / df['سهام کل'].sum()
    df['rate'] = df['rate'].apply(Fnc.to_percentage)
    df = df.groupby('lastTradeDay').apply(Fnc.groupRosob)
    df['rate'] = df['سهام کل'] / df['سهام کل'].sum()
    df['rate'] = df['rate'].apply(Fnc.to_percentage)
    df['index'] = ''
    df['نام خانوادگی '] = ''
    df['نام'] = ''
    df['کد ملی'] = ''
    df['تاریخ تولد'] = ''
    df['محل صدور'] = ''
    df = df.reindex()
    df = df.fillna('')
    dic = {'سهام کل':int(df['سهام کل'].max())}
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df,'dic':dic})



def calculator_fixincom(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})

    prcNowRow1 = int(data['currentPrice'])
    prcFtrRow1 = int(data['futurePrice'])
    dayRow1 = int(data['time'])

    rslRow1 = (prcFtrRow1 / prcNowRow1) - 1
    rslRow1 = rslRow1 ** (365/dayRow1)
    rslRow1 = rslRow1 * 10000
    rslRow1 = int(rslRow1)/100

    rateRow2 = int(data['rate'])
    prcNowRow2 = int(data['current'])
    dayRow2 = int(data['timee'])

    rltRow2 = (rateRow2/100) + 1
    rltRow2 = rltRow2 ** (dayRow2/365)
    rltRow2 = rltRow2 * prcNowRow2
    rltRow2 = round(rltRow2,0)
    return json.dumps({'reply':True,"result1":rslRow1, "result2":rltRow2})


def getcomparetop(data):

    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    etfs = pd.DataFrame(farasahmDb['sandoq'].find({'type':'sabet'},{'_id':0}))
    ts = etfs[etfs['symbol']=='خاتم']
    ts = ts.sort_values(by=['dateInt'],ascending=False)

    etfs = etfs.groupby(by='symbol').apply(Fnc.fund_compare_clu_ccp)
    if data['period'] == "weekly":
        columnsTarget = 'ret_ytm_7'
    elif data['period'] ==  "weeks2":
        columnsTarget = 'ret_ytm_14'
    elif data['period'] == "month1":
        columnsTarget = 'ret_ytm_30'
    elif data['period'] == "month3":
        columnsTarget = 'ret_ytm_365'
    elif data['period'] == "month6":
        columnsTarget = 'ret_ytm_180'
    elif data['period'] == "year":
        columnsTarget = 'ret_ytm_365'
    elif data['period'] == "year2":
        columnsTarget = 'ret_ytm_730'
    else:
        columnsTarget = 'ret_ytm_365'
    
    etfs = etfs.rename(columns={columnsTarget:'ytm'})
    etfs = etfs.sort_values('ytm',ascending=False)
    etfs = etfs.reset_index().drop(columns=['level_1'])
    etfs = etfs[etfs['ytm']>0]
    etfs = etfs[etfs['ytm']<100]
    etfs['rank'] = etfs['ytm'].rank()
    etfs = etfs[['rank','ytm','symbol']]



    top = etfs[etfs['rank']>etfs['rank'].max()-5]
    top.index = top['symbol']
    top = top.to_dict('dict')

    bot = etfs[etfs['rank']<etfs['rank'].min()+5]
    bot = bot.sort_values('rank')
    bot.index = bot['symbol']
    bot = bot.to_dict('dict')

    return json.dumps({'reply':True,'top':top,'bot':bot}) 




def CustomerRemain(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['CustomerRemain'].find({},{'_id':0}))
    df['AdjustedRemain'] = df['AdjustedRemain'].apply(float)
    df = df[df['AdjustedRemain']>0]
    df['datetime'] = df['datetime'].apply(Fnc.gorgianIntToJalali)
    df['lastTradeDay'] = df['lastTradeDateGor'].apply(Fnc.days_difference)
    df = df[df['lastTradeDay']>=60]


    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})


def moadian_getinvoice(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = farasahmDb['invoiceMoadian'].find({},{'result':0,'inquiry':0})
    df = pd.DataFrame(df)
    df['_id'] = df['_id'].apply(str)

    df['date'] = df['date'].apply(Fnc.gorgianIntToJalali)
    df['type'] = [x['header']['inty'] for x in df['invoice']]
    df['pattern'] = [x['header']['inp'] for x in df['invoice']]
    df['buyerType'] = [x['header']['tob'] for x in df['invoice']]
    df['price'] = [x['header']['tadis'] for x in df['invoice']]
    df['taxAdded'] = [x['header']['tvam'] for x in df['invoice']]
    dflistbody = df['invoice'].to_list()
    dflistbody = [x['body'] for x in dflistbody]
    dflistbodyNow = []
    for i in dflistbody:
        listRow = []
        for j in i:
            dic = {
                'fee':j['fee'],
                'taxAdded':j['vam'],
                'price':j['prdis'],
                'sstid':j['sstid'],
                'sstt':j['sstt'],
            }
            listRow.append(dic)
        dflistbodyNow.append(listRow)
    df['_children'] =dflistbodyNow
    df['fee'] = ''
    df['sstid'] = ''
    df['sstt'] = ''
    df = df.drop(columns=['invoice'])
    df = df.drop(columns=['details'])
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})



def valuefundinseris(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbolF},{'_id':0,'VolumeInPrice':1,'date':1}))
    df = df.drop_duplicates()
    df['Volume'] = df['VolumeInPrice'].apply(int)
    df = df.groupby(by='date').sum()
    df['date'] = df.index
    df = df.to_dict('dict')
    return json.dumps({'reply':True,'df':df})


def getassetmixbank(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['bankBalance'].find({'symbol':symbolF}))
    df['return'] = df['return'].replace('yearly',1).replace('monthly',12)
    df = df.groupby(['name']).apply(Fnc.groupBankAsset)
    df['rateBalance'] = (df['rate']/100) * df['balance']
    df['rateBalance'] = df['rateBalance'].apply(int)
    total_rate = (df['rateBalance'].sum() / df['balance'].sum())
    total_rate = round(total_rate * 100,2)
    total_balance = df['balance'].sum()
    df = df.reset_index()[['name','rate','balance']]
    label = [x for x in df['name']]
    lst = [x for x in df['balance']]
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df, 'lab':label, 'lst':lst, 'sum':int(total_balance), 'rate':total_rate})

def customerphonebook(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['registerNoBours'].find({},{'_id':0,'نام و نام خانوادگی':1,'کد ملی':1,'شماره تماس':1}))
    df = df.drop_duplicates(subset=['کد ملی'])
    df = df.dropna(subset=['شماره تماس'])
    df = df.fillna('')
    df['source'] = 'سهامداران'
    df = df.to_dict('records')
    return json.dumps({'reply':True, 'df':df})


def getassetmixoraqdol(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbolF,'type':'gov'},{'Symbol':1,'date':1,'type':1,'VolumeInPrice':1,'Volume':1,'MarketInstrumentTitle':1}))
    df = df[df['date']==df['date'].max()]
    df = df[df['type']!='saham']
    df['rate'] = 0
    for i in df.index:
        name = df['MarketInstrumentTitle'][i]
        rate = farasahmDb['ReturnRateAssetFund'].find_one({'MarketInstrumentTitle': name}, sort=[('date', -1)])
        

        if rate != None:
            df['rate'][i] = round(int(rate['rate']),2)/100
        
    df = df[['Symbol','VolumeInPrice','rate']]
    df['inBal'] = df['VolumeInPrice'] * df['rate']
    df['rate'] = df['rate'] * 100
    total_rate = df['inBal'].sum() / df['VolumeInPrice'].sum()
    total_rate = total_rate * 100
    total_rate = round(total_rate,2)
    total_balance = int(df['VolumeInPrice'].sum())
    label = [x for x in df['Symbol']]
    lst = [x for x in df['VolumeInPrice']]
    df['rate'] = [round(x,2) for x in df['rate']]
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df, 'lab':label, 'lst':lst, 'sum':total_balance, 'rate':total_rate})

def getassetmixoraqnon(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbolF,'type':'non-gov'},{'Symbol':1,'date':1,'type':1,'VolumeInPrice':1,'Volume':1,'MarketInstrumentTitle':1}))
    df = df[df['date']==df['date'].max()]
    df = df[df['type']!='saham']
    df['rate'] = 0
    for i in df.index:
        name = df['MarketInstrumentTitle'][i]
        rate = farasahmDb['ReturnRateAssetFund'].find_one({'MarketInstrumentTitle': name}, sort=[('date', -1)])
        if rate != None:
            df['rate'][i] = round(int(rate['rate']),2)/100
        
    df = df[['Symbol','VolumeInPrice','rate']]
    df['inBal'] = df['VolumeInPrice'] * df['rate']
    df['rate'] = df['rate'] * 100
    total_rate = df['inBal'].sum() / df['VolumeInPrice'].sum()
    total_rate = total_rate * 100
    total_rate = round(total_rate,2)
    total_balance = int(df['VolumeInPrice'].sum())
    label = [x for x in df['Symbol']]
    lst = [x for x in df['VolumeInPrice']]
    df['rate'] = [round(x,2) for x in df['rate']]
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df, 'lab':label, 'lst':lst, 'sum':total_balance, 'rate':total_rate})

def getassetmixoraqsah(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['assetFunds'].find({'Fund':symbolF,'type':'saham'},{'Symbol':1,'date':1,'type':1,'VolumeInPrice':1,'Volume':1,'MarketInstrumentTitle':1}))
    df = df[df['date']==df['date'].max()]
    df = df[df['type']!='non-gov']
    df = df[df['type']!='gov']
    df['rate'] = 0
    for i in df.index:
        name = df['MarketInstrumentTitle'][i]
        rate = farasahmDb['ReturnRateAssetFund'].find_one({'MarketInstrumentTitle': name}, sort=[('date', -1)])
        if rate != None:
            df['rate'][i] = round(int(rate['rate']),2)/100
        
    df = df[['Symbol','VolumeInPrice','rate']]
    df['inBal'] = df['VolumeInPrice'] * df['rate']
    df['rate'] = df['rate'] * 100
    total_rate = df['inBal'].sum() / df['VolumeInPrice'].sum()
    total_rate = total_rate * 100
    total_rate = round(total_rate,2)
    total_balance = int(df['VolumeInPrice'].sum())
    label = [x for x in df['Symbol']]
    lst = [x for x in df['VolumeInPrice']]
    df['rate'] = [round(x,2) for x in df['rate']]
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df, 'lab':label, 'lst':lst, 'sum':total_balance, 'rate':total_rate})


def moadian_delinvoice(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _id = data['id']
    inv = farasahmDb['invoiceMoadian'].find_one({'_id':ObjectId(_id)})
    if inv['inquiry'] == None:
        farasahmDb['invoiceMoadian'].delete_one({'_id':ObjectId(_id)})
        return json.dumps({'reply':True})
    if inv['inquiry']['status'] != 'SUCCESS':
        farasahmDb['invoiceMoadian'].delete_one({'_id':ObjectId(_id)})
        return json.dumps({'reply':True})
    return json.dumps({'reply':False,'msg':'این صورت حساب قبلا با موفقیت به کارپوشه ارسال شده'})


def moadian_print(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})

    a4_width_mm = 297
    a4_height_mm = 210

    # تبدیل میلی‌متر به پیکسل (با فرض DPI = 72)
    dpi = 72
    a4_width_px = int(a4_width_mm * dpi / 25.4)
    a4_height_px = int(a4_height_mm * dpi / 25.4)

    font_path = "public/Peyda-Medium.ttf"
    height_use = 0

    image = Image.new('RGB', ( a4_width_px, a4_height_px ), color='white')
    draw = ImageDraw.Draw(image)

    if acc == None:
        image.save("public/invoice.png")
        return send_file("public/invoice.png", as_attachment=True, mimetype="image/png")
    
    _id = data['id']
    inv = farasahmDb['invoiceMoadian'].find_one({'_id':ObjectId(_id)})
    if inv == None:
        image.save("public/invoice.png")
        return send_file("public/invoice.png", as_attachment=True, mimetype="image/png")
    
    #title
    font = ImageFont.truetype(font_path, 18)
    text = Fnc.repairPersian("صورت حساب فروش")
    width, height = draw.textsize(text, font=font)
    height_margin = 10
    position = ((a4_width_px - width) / 2, height_margin)
    draw.text(position, text, fill="black", font=font)

    #Num&Date
    font = ImageFont.truetype(font_path, 12)
    date = str(Fnc.timestumpToJalalInt(inv['invoice']['header']['indatim']))
    date = date[:4] +'/'+date[4:6]+'/'+date[6:8]
    text = 'شماره:  ' + inv['invoice']['header']['taxid'] + '\n' + 'تاریخ:'+ (' '*100) + date
    text = Fnc.repairPersian(text)
    width, height = draw.textsize(text, font=font)
    height_margin = 10
    position = (10, height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height
    

    #start seller
    height_margin = 10
    height_use = height_use + height_margin
    box_top = height_use

    #title
    font = ImageFont.truetype(font_path, 14)
    text = Fnc.repairPersian("مشخصات فروشنده")
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width) / 2, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height


    #name
    font = ImageFont.truetype(font_path, 13)
    text = Fnc.repairPersian("نام شخص حقوقی: "+inv['details']['seler']['name'])
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)

    #id
    font = ImageFont.truetype(font_path, 13)
    text = Fnc.repairPersian("شناسه ملی: "+inv['details']['seler']['idNum'])
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px/4)-width, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height

    #address
    font = ImageFont.truetype(font_path, 11)
    text = Fnc.repairPersian("ادرس: "+inv['details']['seler']['address'])
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height

    #telephon
    try:
        font = ImageFont.truetype(font_path, 11)
        text = Fnc.repairPersian("تماس: "+inv['details']['seler']['call'])
        width, height = draw.textsize(text, font=font)
        height_margin = 5
        position = ((a4_width_px - width)-20, height_use + height_margin)
        draw.text(position, text, fill="black", font=font)
        height_use = height_use + height_margin + height
    except:
        pass
    
    #draw seller
    height_margin = 8
    box_bottom = height_use + height_margin
    box_left = 10
    box_right  = a4_width_px - 10
    draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
    height_use = height_use + height_margin


    #start Buyer
    height_margin = 10
    height_use = height_use + height_margin
    box_top = height_use

    #title
    font = ImageFont.truetype(font_path, 14)
    text = Fnc.repairPersian("مشخصات خریدار")
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width) / 2, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height

    #name
    font = ImageFont.truetype(font_path, 13)
    text = Fnc.repairPersian("نام شخص حقوقی: "+inv['details']['buyer']['name'])
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    
    #id
    font = ImageFont.truetype(font_path, 13)
    text = inv['invoice']['header']['tins']
    text = Fnc.repairPersian("شناسه ملی: "+text)
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px/4)-width, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height

    #address
    font = ImageFont.truetype(font_path, 11)
    text = Fnc.repairPersian("ادرس: "+inv['details']['buyer']['address'])
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height

    #telephon
    font = ImageFont.truetype(font_path, 11)
    text = Fnc.repairPersian("تماس: "+inv['details']['buyer']['call'])
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height

    #draw seller
    height_margin = 8
    box_bottom = height_use + height_margin
    box_left = 10
    box_right  = a4_width_px - 10
    draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
    height_use = height_use + height_margin


    #start invoice ---------
    height_margin = 12
    height_use = height_use + height_margin
    box_top_invoice = height_use

    #title
    font = ImageFont.truetype(font_path, 14)
    text = Fnc.repairPersian("مشخصات کالا یا خدمات مورد معامله")
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width) / 2, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    height_use = height_use + height_margin + height

    #header tabel
    columns = [
        {'name':'ردیف','prc':4},
        {'name':'کد','prc':10},
        {'name':'شرح','prc':14},
        {'name':'تعداد','prc':5},
        {'name':'قیمت واحد','prc':12},
        {'name':'مبلغ','prc':11},
        {'name':'تخفیف','prc':10},
        {'name':'مبلغ پس از تخفیف','prc':11},
        {'name':'مالیات و عوارض','prc':11},
        {'name':'جمع','prc':12},
    ]
    columns.reverse()
    height_margin = 5
    box_top = height_use + height_margin
    height = 40
    box_bottom = box_top + height
    width_avalibale = a4_width_px - 40
    box_left = 20
    for c in columns:
        text = Fnc.repairPersian(c['name'])
        prc = c['prc'] / 100
        width = prc * width_avalibale
        box_right = box_left + width
        draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
        font = ImageFont.truetype(font_path, 11)
        text_width, text_height = draw.textsize(text, font=font)
        position = (box_left + ((width - text_width ) / 2) , height_use + (height/2) )
        draw.text(position, text, fill="black", font=font)
        box_left = box_right
    height_use = box_bottom

    #row table
    row = inv['invoice']['body']
    rowNum = 1
    listDic = []
    for r in row:
        dic = {'ردیف':rowNum, 'کد':r['sstid'], 'شرح':r['sstt'], 'تعداد':r['am'], 'قیمت واحد':r['fee'],'مبلغ':r['prdis'], 'تخفیف':r['dis'], 'مبلغ پس از تخفیف':r['adis'], 'مالیات و عوارض':r['vam'], 'جمع':r['tsstam']}
        height_margin = 0
        height = 30
        box_top = height_use
        box_bottom = box_top + height
        width_avalibale = a4_width_px - 40
        box_left = 20
        for c in columns:
            text = dic[c['name']]
            if c['name'] not in ['کد','شرح']:
                text = Fnc.add_commas(str(text))
            text = Fnc.repairPersian(str(text))
            prc = c['prc'] / 100
            width = prc * width_avalibale
            box_right = box_left + width
            draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
            font = ImageFont.truetype(font_path, 10)
            text_width, text_height = draw.textsize(text, font=font)
            position = (box_left + ((width - text_width ) / 2) , height_use + (height/2) )
            draw.text(position, text, fill="black", font=font)
            box_left = box_right
        height_use = box_bottom
        rowNum = rowNum + 1
        listDic.append(dic)
    
    #row sum
    height_margin = 0
    height = 30
    box_top = height_use
    box_bottom = box_top + height
    width_avalibale = a4_width_px - 40
    box_left = 20
    for c in columns:
        if c['name'] in ['جمع','مالیات و عوارض','مبلغ پس از تخفیف','تخفیف','مبلغ']:
            text = [int(x[c['name']]) for x in listDic]
            print(dic)
            text = sum(text)
            text = Fnc.add_commas(text)
            text = Fnc.repairPersian(str(text))
            prc = c['prc'] / 100
            width = prc * width_avalibale
            box_right = box_left + width
            draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
            font = ImageFont.truetype(font_path, 10)
            text_width, text_height = draw.textsize(text, font=font)
            position = (box_left + ((width - text_width ) / 2) , height_use + (height/2) )
            draw.text(position, text, fill="black", font=font)
            box_left = box_right
        else:
            pass
    height_use = height_use + height

    #draw invoice box
    height_margin = 15
    box_top = box_top_invoice
    box_bottom = height_use + height_margin
    box_left = 10
    box_right  = a4_width_px - 10
    draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
    height_use = height_use + height_margin

    #discription
    height_margin = 10
    height_use = height_use + height_margin
    box_top = height_use

    font = ImageFont.truetype(font_path, 11)
    text = Fnc.repairPersian("توضیحات: "+inv['details']['text'])
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)
    
    height_box = 25
    box_bottom = height_use + height_box
    box_left = 10
    box_right  = a4_width_px - 10
    draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
    height_use = height_use + height_margin + height_box

    #sing
    height_margin = 10
    height_use = height_use + height_margin
    box_top = height_use

    #seller
    font = ImageFont.truetype(font_path, 11)
    text = Fnc.repairPersian("مهر و امضای فروشنده: ")
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = ((a4_width_px - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)

    #buyer
    font = ImageFont.truetype(font_path, 11)
    text = Fnc.repairPersian("مهر و امضای خریدار: ")
    width, height = draw.textsize(text, font=font)
    height_margin = 5
    position = (((a4_width_px/2) - width)-20, height_use + height_margin)
    draw.text(position, text, fill="black", font=font)

    height_box = 50
    box_bottom = height_use + height_box
    box_left = 10
    box_right  = a4_width_px - 10
    draw.rectangle([box_left, box_top, box_right, box_bottom], outline='black', width=1)
    draw.rectangle([a4_width_px/2, box_top, a4_width_px/2, box_bottom], outline='black', width=1)
    height_use = height_use + height_margin + height_box
        
    image.save("public/invoice.png")
    return send_file("public/invoice.png", as_attachment=True, mimetype="image/png")


def getaccbank(data):
    effective_Acc_Code = [x['Acc_Code'] for x in data['effective']]
    effective_bank = [x['bank'] for x in data['effective']]
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    try:
        date = datetime.datetime.fromtimestamp(int(data['toDay'])/1000)
    except:
        date = datetime.datetime.strptime(data['toDay'], "%Y-%m-%dT%H:%M:%S.%fZ")
    date = JalaliDate(date)
    year = date.year
    group = Fnc.get_groups()
    group['need'] = ['Acc' in x for x in group['db']]
    group = group[group['need']==True]
    group['db'] = group['db'] + str(year)
    group['exists'] = group['db'].apply(Fnc.database_exists)
    group = group[group['exists']==True]
    dff = []
    for i in group.index:
        name_group = group['Name'][i]
        db_group = group['db'][i]
        df = Fnc.clcu_balance_banks(db_group)
        df['eff'] = [(df['Acc_Code'][x] not in effective_Acc_Code) or (df['bank'][x] not in effective_bank)  for x in df.index]
        df['Bede'] = df['Bede'].apply(int)
        df['Best'] = df['Best'].apply(int)
        df['balance'] = df['balance'].apply(int)
        df = df.fillna('')
        
        if len(df)>0:
            balance = df[df['eff']==True]['balance'].sum()
            children = df[['bank','Acc_Code','balance','eff']]
            children['Name'] = name_group
            eff = children['eff'].sum()>0
            children = children.to_dict('records')
            
        else:
            balance = 0
            children = []
            eff = False
        
            
        dic = {'Name':name_group,'balance':int(balance), '_children':children,'eff':str(eff)}
        dff.append(dic)

    return json.dumps({'reply':True, 'df':dff})


def setholliday(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    dates = data['dates']
    dates_jalali = []
    for i in dates:
        di = Fnc.timestumpToJalalInt(i)
        di = str(di)
        y = str(di)[:4]
        m = str(di)[4:6]
        d = str(di)[6:8]
        di = y+'-'+m+'-'+d
        dates_jalali.append({'jajli':di,'timestump':i})
    farasahmDb['hillyday'].delete_many({})
    farasahmDb['hillyday'].insert_many(dates_jalali)
    return json.dumps({'reply':True, 'df':0})




def getholliday(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    df = pd.DataFrame(farasahmDb['hillyday'].find({}))
    df = df['timestump'].to_list()
    
    return json.dumps({'reply':True, 'df':df})
    
    
def fixincom_compareprice(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbolF = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbolF}))
    df = df.dropna()
    df = df[df['dateInt']==df['dateInt'].max()].to_dict('records')[0]
    price = df['final_price']
    ebtal = df['nav']
    amary = df['navAmary']
    dif_ebtal = ebtal - price 
    rate_ebtal = int(((ebtal / price )-1)*10000)/100
    dif_amary = amary - price 
    rate_amary = int(((amary / price) - 1)*10000)/100
    dic = {'price':price, 'ebtal':ebtal, 'amary':amary, 'dif_ebtal':dif_ebtal, 'rate_ebtal':rate_ebtal, 'dif_amary':dif_amary, 'rate_amary':rate_amary}
    return json.dumps({'reply':True, 'dic':dic})
    