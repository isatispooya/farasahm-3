
from flask import send_file

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
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from bson import ObjectId
import Fnc
from ApiMethods import GetCustomerMomentaryAssets
import time

farasahmDb = client['farasahm2']

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
    history = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbol},{'_id':0,'date':1,'final_price':1,'nav':1,'trade_volume':1}))
    history['diff'] = history['nav'] - history['final_price']
    history['rate'] = ((history['nav'] / history['final_price'])-1)*100
    history['rate'] = [round(x,2) for x in history['rate']]
    history['trade_volume'] = [int(x) for x in history['trade_volume']]
    history['date'] = [int(str(x).replace('/','')) for x in history['date']]
    history = history.sort_values(by=['date'],ascending=False)
    dic = {'diff':float(history['diff'].max()), 'rate':float(history['rate'].max()),'volume':float(history['trade_volume'].max())}
    history = history.to_dict('records')
    return json.dumps({'replay':True,'df':history,'dic':dic})



def getreturn(data):
    etf = farasahmDb['menu'].find_one({'name':data['access'][1]})
    name = etf['symbol']
    target = data['input']['target']
    df = pd.DataFrame(farasahmDb['sandoq'].find({'type':'sabet'},{'_id':0}))
    df = df[df['symbol']==name]
    df = df.groupby(by='symbol').apply(Fnc.fund_compare_clu_ccp)
    df = df.reset_index().drop(columns=['level_1']).to_dict('records')[0]
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
        dic[i] = df[i].max()
    df = df.to_dict(orient='records')
    return json.dumps({'replay':True,'df':df,'dic':dic})

def getcompare(data):
    etfs = pd.DataFrame(farasahmDb['sandoq'].find({'type':'sabet'},{'_id':0}))
    etfs = etfs.groupby(by='symbol').apply(Fnc.fund_compare_clu_ccp)
    etfs = etfs.reset_index().drop(columns=['level_1'])

    dic = {}
    for i in etfs.columns:
        if not i == 'symbol':
            dic['i'] = etfs[i].max()
            
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
    df = pd.DataFrame(farasahmDb['Priority'].find({'symbol':symbol},{'enable':0}))
    dfPay = pd.DataFrame(farasahmDb['PriorityPay'].find({"symbol":symbol},{'enable':0}))
    dfPay = dfPay.groupby(by=['frm']).sum(numeric_only=True)
    df = df.set_index('نام و نام خانوادگی').join(dfPay).fillna(0)
    df = df.rename(columns={"popUp":"تعداد واریز","value":"ارزش واریز"})
    df = df.reset_index()
    df['_id'] = df['_id'].astype(str)
    df = df.fillna(0)
    df['countForward'] = df['حق تقدم استفاده شده'] + df['تعداد سهام']
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})



def getprioritytransaction(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['PriorityTransaction'].find({'symbol':symbol},{'popUp':0,'symbol':0}))
    if len(df)==0:
        return json.dumps({'replay':False,'msg':'تراکنشی یافت نشد'})
    
    
    df['_id'] = df['_id'].apply(str)
    df['date'] = df['date'].apply(JalaliDate.to_jalali)
    df['date'] = df['date'].apply(str)
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})



def preemptioncard(data):
    dt = farasahmDb['Priority'].find_one({'symbol':data['sym'],'کد ملی':data['nc']},{'_id':0,'تعداد سهام':0})
    if dt == None:return json.dumps({'replay':False})
    return json.dumps({'replay':True})



def preemptioncardjpg(data):
    dt = farasahmDb['Priority'].find_one({'symbol':data['sym'],'کد ملی':data['nc']},{'_id':0})
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
    textRow2 = 'بنا به تصمیم مجمع عمومی فوق العاده مورخ 1401/08/09 شرکت مبنی بر افزایش سرمایه شرکت از '+ digits.en_to_fa('2000000000000')+ ' ريال \n'
    textRow2 = textRow2 + digits.to_word(2000000000000) + ' به ' + digits.en_to_fa('6000000000000')  +  digits.to_word(6000000000000) + ' ریال ،' + ' شما میتوانید از حق تقدم خود معادل ' + digits.en_to_fa(str(dt['حق تقدم'])) + ' سهم به ارزش اسمی \n' 
    textRow2 = textRow2 + digits.en_to_fa('1000') + ' ریال استفاده نمایید.'  + ' لذا خواهشمند است با توجه به مصوبات موجود که مبلغ ' + digits.en_to_fa(str(int(dt['حق تقدم'])*1000)) + ' ریال به صورت آورده' + '\n'
    textRow2 = textRow2 + ' نقدی طبق اعلامیه پذیرش نویسی در روز نامه پیمان یزد به شماره 5848 مورخ 1402/04/21 و به مدت ' + digits.en_to_fa('60') 
    textRow2 = textRow2 + '\n' + ' روز نسبت به استفاده' + ' حق تقدم و پذیره نویسی تا تاریخ 1402/06/19 اقدام نمایید.'
    textRow2 = arabic_reshaper.reshape(textRow2)
    textRow2 = get_display(textRow2)
    font = ImageFont.truetype(font_path, font_size)
    text_width, text_height = draw.textsize(textRow2, font=font)
    x_position_text = (a4_width - text_width) - 30
    y_position_text = y_position_logo + logo_height_new + 55
    draw.text((x_position_text, y_position_text), textRow2, fill=text_color, font=font,align='right')
    textRow3 = 'بنابر این مبلغ ....................... ريال بر اساس فیش نقدی به شماره ................بانک صادرات حساب شماره \n 0116875409002 به نام شرکت صنایع مفتول ایساتیس پویا به پیوست ارائه میگردد.'
    textRow3 = arabic_reshaper.reshape(textRow3)
    textRow3 = get_display(textRow3)
    text_width, text_height = draw.textsize(textRow3, font=font)
    x_position_text = (a4_width - text_width) - 50
    y_position_text = y_position_logo + logo_height_new + 160
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
    tse_mean = {'نسبت اوراق':df['نسبت اوراق'].mean(), 'نسبت صندوق':df['نسبت صندوق'].mean(), 'نسبت سهام':df['نسبت سهام'].mean(), 'نسبت کل':df['نسبت کل'].mean()}
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
            df[c] = df[c].apply(Fnc.floatTo2decimal)
            df[f'{c} انحراف'] = df[c] - float(Fnc.floatTo2decimal(tse_mean[c]))
            df[f'{c} انحراف'] = df[f'{c} انحراف'].apply(Fnc.floatTo2decimalNormal)
        else:
            df[c] = df[c].apply(Fnc.toBillionRial)
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
    dataList = farasahmDb['deskBrokerVolumeTrade'].distinct('date')
    dataList = [str(x) for x in dataList]
    lastDate = str(max(dataList))
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
    df = pd.DataFrame(farasahmDb['TradeListBroker'].find(
        {'dateInt':date,'TradeSymbolAbs':symbol},
        {'_id':0,'AddedValueTax':0,'BondDividend':0,'BranchID':0,'Discount':0,'InstrumentCategory':0,'MarketInstrumentISIN':0,'page':0,'Update':0,'dateInt':0,
         'صندوق':0,'DateYrInt':0,'DateMnInt':0,'DateDyInt':0,'TradeSymbolAbs':0,'TradeItemBroker':0,'TradeItemRayanBourse':0,'TradeNumber':0,'TradeSymbol':0,'نام':0}))
    if len(df) == 0:
        return json.dumps({'replay':False,'msg':'گزارشی یافت نشد'})
    df = df[pd.to_numeric(df['NetPrice'], errors='coerce').notnull()]
    df = df.groupby(by=['TradeCode']).apply(Fnc.Apply_Trade_Symbol,symbol = symbol,date=date)
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
    if len(bank)>0:
        bank = bank.rename(columns={'balance':'value'})
        bank['type'] = 'سپرده بانکی'
        bank = bank[['name','value','type']]
        df = pd.concat([asset,bank])
    else:
        df = asset
    df['type'] = df['type'].replace('saham','سهام').replace('non-gov','اوراق شرکتی').replace('gov','اوراق دولتی')
    df['value'] = df['value'].apply(int)
    df['rate'] = df['value'] / df['value'].sum()
    df['warnint'] = ''
    df = df.reset_index().drop(columns=['index'])
    for i in df.index:
        if df['type'][i] == 'سپرده بانکی' and df['value'][i] / df['value'].sum() > 0.133:
            df['warnint'][i] = 'این سپرده بیش از 13.3% است'
        elif df['type'][i] == 'سپرده بانکی' and df[df['type']=='سپرده بانکی']['value'].sum()/df['value'].sum() > 0.40:
            df['warnint'][i] = 'مجموع سپرده های بانکی بیش از 40% است'
        elif df['type'][i] == 'اوراق دولتی' and df[df['type']=='اوراق دولتی']['value'].sum()/df['value'].sum() > 0.30:
            df['warnint'][i] = 'مجموع اوراق دولتی بیش از 30% است'
        elif df['type'][i] == 'اوراق دولتی' and df[df['type']=='اوراق دولتی']['value'].sum()/df['value'].sum() < 0.25:
            df['warnint'][i] = 'مجموع اوراق دولتی کمتر از 25% است'
        elif 'اوراق' in df['type'][i] and (df[df['type']=='اوراق دولتی']['value'].sum() + df[df['type']=='اوراق شرکتی']['value'].sum()) /df['value'].sum() < 0.40:
            df['warnint'][i] = 'مجموع اوراق کمتر از 40% است'
        elif df['type'][i] == 'سهام' and df['value'][i] / df['value'].sum() > 0.03:
            df['warnint'][i] = 'این سهم  بیش از 3% است'
        elif df['type'][i] == 'سهام' and df[df['type']=='سهام']['value'].sum()/df['value'].sum() > 0.1:
            df['warnint'][i] = 'مجموع کل سهم بیش از 10% است'
        else:
            df['warnint'][i] = ''
    df['rate'] = (df['rate'] * 10000).apply(int)/100
    df = df.to_dict('records')
    return json.dumps({'reply':True,'df':df})



def getoraghytm(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['oraghYTM'].find({},{'_id':0,'بازده ساده':0}))
    df['LastDay'] = df['تاریخ سررسید'].apply(Fnc.jalaliStrDifToday)
    df = df.fillna(0)
    dic = {'YTM':int(df['YTM'].max()),'LastDay':int(df['LastDay'].max())}
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
    df = pd.DataFrame(farasahmDb['sandoq'].find({'symbol':symbol},{'_id':0,'dateInt':1,'final_price':1}))
    lastUpdate = df['dateInt'].max()
    df = df[df['dateInt']==lastUpdate]
    df['dateInt'] = [int(x) for x in df['dateInt']]
    df = df.set_index('dateInt')
    dateDf = pd.DataFrame(Fnc.calnder())
    dateDf['dateInt'] = [int(x.replace('-','')) for x in dateDf['ja_date']]
    dateDf = dateDf.set_index('dateInt')
    df = df.join(dateDf,how='outer').reset_index()
    df = df[df['dateInt']>=lastUpdate].reset_index().drop(columns=['index'])
    df['future_holidays'] = df.apply(lambda row: Fnc.calculate_future_holidays(row, df), axis=1)
    df['past_holidays'] = df.apply(lambda row: Fnc.calculate_past_holidays(row, df), axis=1)
    grow_rate = ((int(data['target'])/100)+1) ** (1/365)
    befor_grow_rate = ((int(data['befor'])/100) * (grow_rate - 1)) + 1
    after_grow_rate = ((int(data['after'])/100) * (grow_rate - 1)) + 1
    df['grow_rate'] = grow_rate #** df.index
    df['grow_holiday_fut'] =  befor_grow_rate ** df['future_holidays']
    df['grow_holiday_pas'] =  after_grow_rate ** df['past_holidays']
    df['grow_Fin'] = (df['grow_rate'] * df['workday']) * df['grow_holiday_fut'] * df['grow_holiday_pas']
    df['grow_Fin'] = df['grow_Fin'].replace(0,np.NaN)
    df['final_price'] = df['final_price'].fillna(method='ffill')
    df['grow_Fin'] = df['grow_Fin'].fillna(1)
    df['grow_Fin'] = df['grow_Fin'].cumprod()
    df['fut_price'] = df['final_price'] * df['grow_Fin']
    df['fut_price'] = df['fut_price'].apply(round)
    df = df[df.index<=365]
    df = df[['ja_date','week','workday','fut_price']]
    df['week'] = df['week'].replace(0,'شنبه').replace(1,'یکشنبه').replace(2,'دوشنبه').replace(3,'سه شنبه').replace(4,'چهارشنبه').replace(5,'پنج شنبه').replace(6,'جمعه')
    df['workday'] = df['workday'].replace(True,'کاری').replace(False,'تعطیل')
    df['Chng_price'] = ((df['fut_price'] / df['fut_price'].shift(1)) - 1) * 100000
    df['Chng_price'] = df['Chng_price'].fillna(0)
    df['Chng_price'] = df['Chng_price'].apply(int)/1000
    df = df.to_dict('records')
    return json.dumps({'reply':True, 'df':df})
