
import json
import zipfile
import pandas as pd
from io import StringIO 
from bson import ObjectId
import Fnc
import pymongo
from persiantools.jdatetime import JalaliDate
import datetime
from Login import adminCheck
from sklearn.linear_model import LinearRegression

client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

def zipToDf(zip):
    df = dfDaily = zipfile.ZipFile(zip, 'r')
    df = df.read(df.namelist()[0])
    df = str(df, 'utf-8')
    df = StringIO(df)
    df = pd.read_csv(df, sep='|')
    return df

columnsDaily = ['نماد', 'نماد کدال', 'نام نماد', 'شماره اعلامیه', 'تاریخ معامله','کد خریدار', 'کد سهامداری خریدار تبدیل نشده', 'نام خانوادگی خریدار','نام خریدار', 'سری شناسنامه خریدار', 'سریال شناسنامه خریدار','کد ملی خریدار', 'ش ثبت/ش .شناسنامه خریدار', 'محل صدور خریدار','نام پدر خریدار', 'نوع خریدار', 'نوع سهام خریدار', 'کد فروشنده','کد سهامداری فروشنده تبدیل نشده', 'نام خانوادگی فروشنده', 'نام فروشنده','سری شناسنامه فروشنده', 'سریال شناسنامه فروشنده', 'کد ملی فروشنده','ش ثبت/ش .شناسنامه فروشنده', 'محل صدور فروشنده', 'نام پدر فروشنده','نوع فروشنده', 'نوع سهام فروشنده', 'تعداد سهم', 'قیمت هر سهم','کد کارگزار خریدار', 'نام کارگزار خریدار', 'کد کارگزار فروشنده','نام کارگزار فروشنده']
columnsRegister = ['نماد', 'نماد کدال', 'تاریخ گزارش', 'کد سهامداری','کدسهامداری تبدیل نشده', 'سهام کل', 'سهام سپرده', 'سهام غیرسپرده','نام خانوادگی ', 'نام', 'fullName', 'کد ملی', 'شماره ثبت/شناسنامه',       'تاریخ تولد', 'نام پدر', 'محل صدور', 'سری', 'سریال', 'جنسیت', 'نوع','نوع سهامدار', 'نوع سهام', 'شناسه ملی', 'فرمت قدیم کد سهامداری','سود کدهای وثیقه']

def Update(access,daily,registerdaily):
    dfDaily = zipToDf(daily)
    dfRegister = zipToDf(registerdaily)
    if len(dfDaily)==0: return json.dumps({'replay':False,'msg':'فایل معاملات خالی است'})
    if len(dfRegister)==0: return json.dumps({'replay':False,'msg':'فایل رجیستر خالی است'})
    for i in columnsDaily:
        if i not in dfDaily.columns:return json.dumps({'replay':False,'msg':f'فایل معاملات فاقد ستون {i} است'})
    for i in columnsRegister:
        if i not in dfRegister.columns:return json.dumps({'replay':False,'msg':f'فایل رجیستر فاقد ستون {i} است'})
    dateDaily = list(set(dfDaily['تاریخ معامله']))
    dateregister = list(set(dfRegister['تاریخ گزارش']))
    if dateDaily != dateregister: return json.dumps({'replay':False,'msg':'تاریخ گزارش فایل ها برابر نیست'})
    for i in dateDaily:
        farasahmDb['trade'].delete_many({'تاریخ معامله':i,'symbol':str(access).split(',')[1]})
        farasahmDb['register'].delete_many({'تاریخ گزارش':i,'symbol':str(access).split(',')[1]})
    dfDaily['user'] = str(access).split(',')[0]
    dfRegister['user'] = str(access).split(',')[0]
    dfDaily['symbol'] = str(access).split(',')[1]
    dfRegister['symbol'] = str(access).split(',')[1]
    farasahmDb['trade'].insert_many(dfDaily.to_dict('records'))
    #برای ثبت اخرین ایستگاه های خرید و فروش سهامداران
    dfTradeStation = pd.DataFrame(farasahmDb['trade'].find({'symbol':str(access).split(',')[1]},{'_id':0,'تاریخ معامله':1,'کد خریدار':1,'نام کارگزار خریدار':1,'کد فروشنده':1,'نام کارگزار فروشنده':1}))
    dfTradeStation = dfTradeStation.sort_values('تاریخ معامله',ascending=False)
    dfTradeStationBy = dfTradeStation.copy()[['کد خریدار','نام کارگزار خریدار']]
    dfTradeStationSel = dfTradeStation.copy()[['کد فروشنده','نام کارگزار فروشنده']]
    dfTradeStationBy = dfTradeStationBy.drop_duplicates(subset=['کد خریدار'],keep='first')
    dfTradeStationSel = dfTradeStationSel.drop_duplicates(subset=['کد فروشنده'],keep='first')

    list_dfRegister = dfRegister['کد سهامداری'].to_list()

    conditional_by = dfTradeStationBy['کد خریدار'].isin(list_dfRegister)
    conditional_sl = dfTradeStationSel['کد فروشنده'].isin(list_dfRegister)

    dfTradeStationBy = dfTradeStationBy[conditional_by]
    dfTradeStationSel = dfTradeStationSel[conditional_sl]
    
    dfTradeStationBy = dfTradeStationBy.rename(columns={'کد خریدار':'کد سهامداری','نام کارگزار خریدار':'اخرین کارگزاری خرید'})
    dfTradeStationSel = dfTradeStationSel.rename(columns={'کد فروشنده':'کد سهامداری','نام کارگزار فروشنده':'اخرین کارگزاری فروش'})
    dfRegister = dfRegister.set_index('کد سهامداری').join(dfTradeStationBy.set_index('کد سهامداری'),how='left')
    dfRegister = dfRegister.join(dfTradeStationSel.set_index('کد سهامداری'),how='left').reset_index()
    dfRegister['اخرین کارگزاری خرید'] = dfRegister['اخرین کارگزاری خرید'].fillna('ایساتیس پویا')
    dfRegister['اخرین کارگزاری فروش'] = dfRegister['اخرین کارگزاری فروش'].fillna(dfRegister['اخرین کارگزاری خرید'])
    dfRegister = dfRegister.fillna('')
    brokerList = farasahmDb['borkerList'].find({},{'_id':0})
    for brk in brokerList:
        name = list(brk.keys())[0]
        members = brk[name]
        for m in members:
            dfRegister.loc[dfRegister["اخرین کارگزاری خرید"].str.contains(m), "اخرین کارگزاری خرید"] = m
            dfRegister.loc[dfRegister["اخرین کارگزاری فروش"].str.contains(m), "اخرین کارگزاری فروش"] = m


    farasahmDb['register'].insert_many(dfRegister.to_dict('records'))
    return json.dumps({'replay':True,'date':dateDaily})


def createTraders(data):
    symbol = data['access'][1]
    for i in data['date']:
        #در کاکشن traders برای نماد و تاریخی که داریم روش کار میکنم اگر از قبل در موجود بوده حذف میکنم که اطلاعات جدید جایگزین کنیم
        farasahmDb['traders'].delete_many({'symbol':symbol,'date':i})
        #اطلاعات خام معهاملات رو از کالکشن trade گرفتیم
        dfTrade = pd.DataFrame(farasahmDb['trade'].find({"تاریخ معامله":i,'symbol':symbol},{'_id':0}))
        #مقدار ارزش معاملات رو محاسبه کردیم
        dfTrade['Value'] = dfTrade['تعداد سهم'] * dfTrade['قیمت هر سهم']

        #دیتا فریم جدید برای خریداران و فروشندگان ایجاد کردیم و مقدار ارزش معاملات و حجمشون رو تجمیع کردیم
        dfBuy = dfTrade.groupby('کد خریدار').sum()
        dfBuy = dfBuy[['تعداد سهم','Value']].reset_index()

        dfSell = dfTrade.groupby('کد فروشنده').sum()
        dfSell = dfSell[['تعداد سهم','Value']].reset_index()

        dfBuy.columns = ['کد','تعداد خرید','ارزش خرید']
        dfSell.columns = ['کد','تعداد فروش','ارزش فروش']


        df = pd.concat([dfBuy,dfSell])
        df = df.fillna(0)
        df = df.groupby('کد').sum()

        dfTrade = dfTrade[['کد خریدار','محل صدور خریدار' ,'محل صدور فروشنده', 'نام خانوادگی خریدار','نام خریدار','کد فروشنده', 'نام خانوادگی فروشنده', 'نام فروشنده','نام کارگزار خریدار','نام کارگزار فروشنده', ]]
        dfBuy = dfTrade[['کد خریدار','محل صدور خریدار' ,'نام خانوادگی خریدار','نام خریدار','نام کارگزار خریدار']]
        dfSell = dfTrade[['کد فروشنده','محل صدور فروشنده','نام خانوادگی فروشنده', 'نام فروشنده','نام کارگزار فروشنده']]
        dfBuy.columns = ['کد','صدور' ,'نام خانوادگی','نام','نام کارگزار']
        dfSell.columns = ['کد','صدور' ,'نام خانوادگی','نام','نام کارگزار']
        dff = pd.concat([dfBuy,dfSell]).fillna('').set_index('کد')

        dff['fullname'] = dff['نام'] +' ' + dff['نام خانوادگی']

        dff = dff.drop(columns=['نام خانوادگی','نام'])
        df = df.join(dff,how='left')
        df = df.reset_index()
        df = df.drop_duplicates(subset=['کد'])

        df['avgBuy'] = df['ارزش خرید'] / df['تعداد خرید']
        df['avgSell'] = df['ارزش فروش'] / df['تعداد فروش']

        register = pd.DataFrame(farasahmDb['register'].find({"تاریخ گزارش":i,'symbol':symbol},{'_id':0,'سهام کل':1,'کد سهامداری':1}))
        register.columns = ['کد', 'سهام کل']
        register = register.drop_duplicates(subset=['کد'])
        register = register.set_index('کد')
        df = df.set_index('کد')
        df = df.join(register,how='left')
        df = df.fillna(0).reset_index()
        df['avgBuy'] = df['avgBuy'].apply(int)
        df['avgSell'] = df['avgSell'].apply(int)
        df = df.drop(columns=['ارزش خرید','ارزش فروش'])
        df['date'] = i
        df['symbol'] = symbol
        df = df.to_dict('records')
        farasahmDb['traders'].insert_many(df)
    return json.dumps({'replay':True})

def createNewTraders(data):
    symbol = data['access'][1]
    dfTrader = pd.DataFrame(farasahmDb['traders'].find({'symbol':symbol},{'_id':0,'date':1,'سهام کل':1,'تعداد فروش':1,'نام کارگزار':1,'تعداد خرید':1,'کد':1,'fullname':1}))
    for i in data['date']:
        oldLidt = list(set(dfTrader[dfTrader['date']<i]['کد']))
        dfNew = dfTrader[dfTrader['date']==i]
        dfNew = dfNew[dfNew['تعداد خرید']>0]
        dfNew['new'] = dfNew['کد'].isin(oldLidt)
        dic = {'date':i,'allVol':dfNew['تعداد خرید'].sum(),'allCntBuy':len(dfNew)}
        dfNew = dfNew[dfNew['new']==False]
        dic['newVol'] = dfNew['تعداد خرید'].sum()
        dic['newCnt'] = len(dfNew['تعداد خرید'])
        dic['ratNewVol'] = dic['newVol'] / dic['allVol']
        dic['ratNewCnt'] = dic['newCnt'] / dic['allCntBuy']
        dfNew = dfNew[['کد','تعداد خرید','تعداد فروش','نام کارگزار','سهام کل','fullname']]
        dfNew = dfNew.to_dict('records')
        dic['newcomer'] = dfNew
        dic['symbol'] = symbol
        out = pd.DataFrame(farasahmDb['traders'].find({'symbol':symbol,'date':i},{'_id':0,'date':1,'سهام کل':1,'تعداد فروش':1,'نام کارگزار':1,'تعداد خرید':1,'کد':1,'fullname':1}))
        out = out[out['تعداد فروش']>=0]
        dic['allCntSel'] = len(out['تعداد فروش'])
        out = out[out['سهام کل']<=0]
        dic['outVol'] = out['تعداد فروش'].sum()
        dic['outCnt'] = len(out['تعداد فروش'])
        dic['ratOutVol'] = dic['outVol'] / dic['allVol']
        dic['ratOutCnt'] = dic['outCnt'] / dic['allCntSel']
        out = out[['کد','تعداد خرید','تعداد فروش','نام کارگزار','سهام کل','fullname']]
        out = out.to_dict('records')
        dic['runway'] = out
        farasahmDb['newComer'].delete_many({'date':i,'symbol':symbol})
        farasahmDb['newComer'].insert_one(dic)
    return json.dumps({'replay':True})


def createStation(data):
    symbol = data['access'][1]
    for i in data['date']:
        dfTrade = pd.DataFrame(farasahmDb['trade'].find({"تاریخ معامله":i,'symbol':symbol},{'_id':0,'تعداد سهم':1,'قیمت هر سهم':1,'نام کارگزار خریدار':1,'نام کارگزار فروشنده':1}))
        dfTrade['value'] = dfTrade['تعداد سهم'] * dfTrade['قیمت هر سهم']
        dfB = dfTrade[['تعداد سهم','نام کارگزار خریدار','value']]
        dfS = dfTrade[['تعداد سهم','نام کارگزار فروشنده','value']]
        dfB.columns = ['تعداد خرید', 'نام کارگزار', 'ارزش خرید']
        dfS.columns = ['تعداد فروش', 'نام کارگزار', 'ارزش فروش']
        df = pd.concat([dfB,dfS]).groupby('نام کارگزار').sum()
        df['قیمت خرید'] = df['ارزش خرید'] / df['تعداد خرید']
        df['قیمت فروش'] = df['ارزش فروش'] / df['تعداد فروش']
        df = df.reset_index().drop(columns=['ارزش خرید','ارزش فروش']).fillna(0)
        df['قیمت خرید'] = [int(x) for x in df['قیمت خرید']]
        df['قیمت فروش'] = [int(x) for x in df['قیمت فروش']]
        df['date'] = i
        df['symbol'] = symbol
        farasahmDb['station'].delete_many({'symbol':symbol,'date':i})
        farasahmDb['station'].insert_many(df.to_dict('records'))
    return json.dumps({'replay':True})

def createBroker(data):
    symbol = data['access'][1]
    brokerList = [x for x in farasahmDb['borkerList'].find({},{'_id':0})]
    for i in data['date']:
        station = pd.DataFrame(farasahmDb['station'].find({'symbol':symbol,'date':i},{'_id':0}))
        station['broker'] = ''
        station['buyValue'] = station['تعداد خرید'] * station['قیمت خرید']
        station['selValue'] = station['تعداد فروش'] * station['قیمت فروش']
        station = station.drop(columns=['قیمت خرید','قیمت فروش'])
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
        station = station.groupby('broker').sum().reset_index()
        station['قیمت خرید'] = station['buyValue'] / station['تعداد خرید']
        station['قیمت فروش'] = station['selValue'] / station['تعداد فروش']
        station['قیمت خرید'] = station['قیمت خرید'].fillna(0)
        station['قیمت فروش'] = station['قیمت خرید'].fillna(0)
        station['قیمت خرید'] = [int(x) for x in station['قیمت خرید']]
        station['قیمت فروش'] = [int(x) for x in station['قیمت فروش']]
        station = station.drop(columns=['buyValue','selValue'])
        station['date'] = i
        station['symbol'] = symbol
        farasahmDb['broker'].delete_many({'symbol':symbol,'date':i})
        farasahmDb['broker'].insert_many(station.to_dict('records'))
    return json.dumps({'replay':True})

def createholder(data):
    symbol = data['access'][1]
    dfTrader = pd.DataFrame(farasahmDb['traders'].find({'symbol':symbol},{'_id':0,'date':1,'کد':1}))
    dfRegister = pd.DataFrame(farasahmDb['register'].find({'symbol':symbol},{'_id':0,'تاریخ گزارش':1,'کد سهامداری':1,'fullName':1,'سهام کل':1}))
    for i in data['date']:
        gorgia = JalaliDate(int(str(i)[:4]), int(str(i)[4:6]), int(str(i)[6:])).to_gregorian()
        for j in [3,6,12,18,24]:
            baseGorgia = gorgia - datetime.timedelta(days=j*30)
            basicJalali = int(str(JalaliDate.to_jalali(baseGorgia.year,baseGorgia.month,baseGorgia.day)).replace('-',''))
            active = list(set(dfTrader[dfTrader['date']>=basicJalali]['کد']))
            dfRegisterDisabale = dfRegister[dfRegister['تاریخ گزارش']<basicJalali]
            if len(dfRegisterDisabale)>0:
                dfRegisterDisabale = dfRegisterDisabale[dfRegisterDisabale['تاریخ گزارش']==dfRegisterDisabale['تاریخ گزارش'].max()]
                dfRegisterDisabale['active'] = dfRegisterDisabale['کد سهامداری'].isin(active)
                dfRegisterDisabale = dfRegisterDisabale[dfRegisterDisabale['active']==False]
                if len(dfRegisterDisabale)>0:
                    dfRegisterDisabale = dfRegisterDisabale[['fullName','سهام کل']]
                    dfRegisterDisabale['date'] = i
                    dfRegisterDisabale['period'] = j
                    dfRegisterDisabale['symbol'] = symbol
                    farasahmDb['holder'].delete_many({'symbol':symbol,'period':j,'date':i})
                    farasahmDb['holder'].insert_many(dfRegisterDisabale.to_dict('records'))
    return json.dumps({'replay':True})

def mashinlearninimg(data):
    '''
        ChgRtAvgCntSel50 => changeRate(avg(count(seller),50))
        MnPrc20ToPrc => min(price,20)/price
        AvgPrc50ToPrc => avg(price,50)/price
        ChgStdPrc50 => change(std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50))
        AvgPrc50ToPrc => avg(price,20)/price
        MnMxPrc20ToMacPrc =>min(max(price),20)/max(price)
        MnMnPrc20ToMnPrc => min(min(price),20)/min(price)
        AvgMxPrc20ToMxPrc => avg(max(price),20)/max(price)
        CngMxCntSel5ToMCntSel5 => change(max(count(seller),5)/min(count(seller),5))
        CngAvgCntSel50 => change(avg(count(seller),50))
        CngRtAvgMxPrc20 => changeRate(avg(max(price),20))
    '''
    # trade => date, volume, value saller price
    # newComer => date, allCntSel



    symbol = data['access'][1]
    for i in data['date']:
        df = pd.DataFrame(farasahmDb['trade'].find({'symbol':symbol},{'_id':0,'تاریخ معامله':1,'تعداد سهم':1,'قیمت هر سهم':1}))
        df.columns = ['date', 'volume', 'price']
        df = df[df['date']<=i]

        # در اینجا ما حداکثر به 50 روز کار اخیر نیاز داریم که اینجوری مابقی روز ها رو حذف کردیم
        lastDate50 = sorted(list(set(df['date'])),reverse=True)[:50]
        df = df[df['date']>=min(lastDate50)]
        
        df['value'] = df['volume'] * df['price']

        minDf = df.groupby('date').min()[['price']]
        minDf.columns = ['min(price)']
        maxDf = df.groupby('date').max()
        maxDf.columns = ['max(volume)','max(price)','max(value)']
        avgDf = df.groupby('date').mean()
        avgDf.columns = ['avg(volume)','avg(price)','avg(value)']
        df = df.groupby('date').sum()
        sallerDf = pd.DataFrame(farasahmDb['newComer'].find({'symbol':symbol},{'_id':0,'date':1,'allCntSel':1,'newCnt':1}))
        sallerDf =sallerDf[sallerDf['date']<=i]
        sallerDf =sallerDf[sallerDf['date']>=min(lastDate50)]
        sallerDf.columns = ['date','countNewBuyer','count(seller)']
        df = df.join(minDf).join(maxDf).join(avgDf).join(sallerDf.set_index('date'))
        # create => (max(price)-min(price))/avg(price) , price
        df['(max(price)-min(price))/avg(price)'] = (df['max(price)'] - df['min(price)']) / df['avg(price)']
        df['price'] = df['value'] / df['volume']
        df = df.sort_index()
        # avg : 50 => [count(seller) , price, (max(price)-min(price))/avg(price)]  20 => [price , max(price)]
        df['avg(count(seller),50)'] = df['count(seller)'].rolling(window=50,min_periods=1).mean()
        df['avg(price,50)'] = df['price'].rolling(window=50,min_periods=1).mean()
        df['avg(price,20)'] = df['price'].rolling(window=20,min_periods=1).mean()
        df['avg((max(price)-min(price))/avg(price),50)'] = df['(max(price)-min(price))/avg(price)'].rolling(window=50,min_periods=1).mean()
        df['avg(max(price),20)'] = df['max(price)'].rolling(window=20,min_periods=1).mean()
        # std : 50 => [(max(price)-min(price))/avg(price)]
        df['std((max(price)-min(price))/avg(price),50)'] = df['(max(price)-min(price))/avg(price)'].rolling(window=50,min_periods=1).std()
        # min : 20 => [price, max(price) , min(price)] 5 => [count(seller)]
        df['min(price,20)'] = df['price'].rolling(window=20,min_periods=1).min()
        df['min(max(price),20)'] = df['max(price)'].rolling(window=20,min_periods=1).min()
        df['min(min(price),20)'] = df['min(price)'].rolling(window=20,min_periods=1).min()
        df['min(count(seller),5)'] = df['count(seller)'].rolling(window=5,min_periods=1).min()
        # max : 5 => [count(seller)]
        df['max(count(seller),5)'] = df['count(seller)'].rolling(window=5,min_periods=1).max()
        # create => std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50), avg(max(price),20)/max(price)
        df['std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50)'] = df['std((max(price)-min(price))/avg(price),50)'] / df['avg((max(price)-min(price))/avg(price),50)']
        df['avg(max(price),20)/max(price)'] = df['avg(max(price),20)'] / df['max(price)']
        # create => max(count(seller),5)/min(count(seller),5)
        df['max(count(seller),5)/min(count(seller),5)'] = df['max(count(seller),5)'] / df['min(count(seller),5)']

        # changeRate => [avg(count(seller),50),avg(max(price),20)]
        df['changeRate(avg(count(seller),50))'] = (df['avg(count(seller),50)'].shift(-1) / df['avg(count(seller),50)']).fillna(0)
        df['changeRate(avg(max(price),20))'] = (df['avg(max(price),20)'].shift(-1) / df['avg(max(price),20)']).fillna(0)
        # change => [std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50), avg(max(price),20)/max(price), max(count(seller),5)/min(count(seller),5), avg(count(seller),50)]
        df['change(std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50))'] = (df['std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50)'].shift(-1) - df['std((max(price)-min(price))/avg(price),50)/avg((max(price)-min(price))/avg(price),50)']).fillna(0)
        df['change(max(count(seller),5)/min(count(seller),5))'] = (df['max(count(seller),5)/min(count(seller),5)'].shift(-1) - df['max(count(seller),5)/min(count(seller),5)']).fillna(0)
        df['change(avg(count(seller),50))'] = (df['avg(count(seller),50)'].shift(-1) - df['avg(count(seller),50)']).fillna(0)
        # create => min(price,20)/price , avg(price,50)/price , avg(price,20)/price , min(max(price),20)/max(price), min(min(price),20)/min(price)
        df['min(price,20)/price'] = df['min(price,20)'] / df['price']
        df['avg(price,50)/price'] = df['avg(price,50)'] / df['price']
        df['avg(price,20)/price'] = df['avg(price,20)'] / df['price']
        df['min(max(price),20)/max(price)'] = df['min(max(price),20)'] / df['max(price)']
        df['min(min(price),20)/min(price)'] = df['min(min(price),20)'] / df['min(price)']
        # create => avg(max(price),20)/max(price)
        df['avg(max(price),20)/max(price)'] = df['avg(max(price),20)'] / df['max(price)']
        df = df.fillna(df.mean())
        df = df[df.index==df.index.max()].reset_index()
        df = df.to_dict('records')[0]
        df['symbol'] = symbol
        # insert Db
        farasahmDb['features'].delete_many({'symbol':symbol,'date':i})
        farasahmDb['features'].insert_one(df)
        # predict
        df = pd.DataFrame(farasahmDb['features'].find({'symbol':symbol},{'_id':0,'predict_CountNewBuyer':0}))
        df = df[df['date']<=i]
        # برای پیشبینی حداقل نیاز به 50 روز سابقه معاملاتی است
        if len(df)<=50:return json.dumps({'replay':True})
        modelRegression = LinearRegression()
        # predict newComer
        df['countNewBuyer'] = df['countNewBuyer'].shift(-1)
        dftrain = df[df['date']<i]
        dftest = df[df['date']==i]
        x_train = dftrain[['changeRate(avg(count(seller),50))','min(price,20)/price','avg(price,20)/price','avg(price,50)/price']]
        y_train = dftrain[['countNewBuyer']]
        x_test = dftest[['changeRate(avg(count(seller),50))','min(price,20)/price','avg(price,20)/price','avg(price,50)/price']]
        modelRegression.fit(x_train, y_train)
        y_pred = modelRegression.predict(x_test)[0][0]

        farasahmDb['features'].update_one({'symbol':symbol,'date':i},{'$set':{'predict_CountNewBuyer':y_pred}})
    return json.dumps({'replay':True})

def lastupdate(data):
    symbol = data['access'][1]
    resultList = farasahmDb['trade'].find({'symbol':symbol},{"تاریخ معامله":1})
    resultList =[x['تاریخ معامله'] for x in resultList]
    if len(resultList) == 0: result = '-'
    else:
        result = max(resultList)
        resultInt = int(result)
        resultstr = str(result)
        resultslash = resultstr[:4]+'/'+resultstr[4:6]+'/'+resultstr[6:]
        gorgia = str(JalaliDate(int(str(resultstr)[:4]), int(str(resultstr)[4:6]), int(str(resultstr)[6:])).to_gregorian())
        resultList = [str(x) for x in resultList]
    return json.dumps({'replay':True,'resultslash':resultslash,'resultInt':resultInt,'gorgia':gorgia,'resultList':resultList})



def setgrouping(data):
    dic = {'symbol':data['access'][1], 'nameGroup':data['name'], 'members':[x['کد'] for x in data['members']], 'user':data['access'][0]}
    if farasahmDb['grouping'].find_one({'nameGroup':dic['nameGroup'],'symbol':data['access'][1]}) != None:
        farasahmDb['grouping'].delete_many({'nameGroup':dic['nameGroup']})
    allgroup = farasahmDb['grouping'].find({'symbol':data['access'][1]})
    allgroup = [x['members'] for x in allgroup]
    allgroupMarge = []
    for i in allgroup:
        allgroupMarge = allgroupMarge + i
    for i in dic['members']:
        if i in allgroupMarge:
            return json.dumps({'replay':False, 'msg':f'کد {i} قبلا در یک گروه قرار گرفته است'})
    farasahmDb['grouping'].insert_one(dic)
    return json.dumps({'replay':True})

def delrowgrouping(data):
    farasahmDb['grouping'].delete_many({'nameGroup':data['ditail']['name'],'symbol':data['access'][1]})
    return json.dumps({'replay':True})

def setTransaction(data):
    symbol = data['access'][1]
    data['dataTrade']['volume'] = int(data['dataTrade']['volume'])
    data['dataTrade']['price'] = int(data['dataTrade']['price'])
    data['dataTrade']['value'] = int(data['dataTrade']['value'])
    data['dataTrade']['symbol'] = symbol
    Tansaction = farasahmDb['transactions'].find_one({'symbol':symbol,'id':int(data['dataTrade']['id'])})
    if Tansaction == None:
        dfRegister = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol},{'_id':0}))
        lastDate = dfRegister['date'].max()
        toDay = int(str(JalaliDate.today()).replace('-',''))
        dfRegister = dfRegister[dfRegister['date']==lastDate]

        dfRegister = dfRegister.set_index('نام و نام خانوادگی')
        balanceSaller = dfRegister['تعداد سهام'][data['dataTrade']['sell']] - int(data['dataTrade']['volume'])
        balanceBuyer = dfRegister['تعداد سهام'][data['dataTrade']['buy']] + int(data['dataTrade']['volume'])
        if balanceSaller<0:return json.dumps({'replay':False,'msg':'مانده فروشنده کافی نمیباشد'})
        dfRegister['تعداد سهام'][data['dataTrade']['sell']] = balanceSaller
        dfRegister['تعداد سهام'][data['dataTrade']['buy']] = balanceBuyer
        dfRegister['date'] = toDay
        dfRegister = dfRegister.reset_index()
        data['dataTrade']['date'] = toDay
        farasahmDb['registerNoBours'].delete_many({'symbol':symbol,'date':toDay})
        farasahmDb['registerNoBours'].insert_many(dfRegister.to_dict('records'))
        farasahmDb['transactions'].insert_one(data['dataTrade'])
        return json.dumps({'replay':True})
    else:
        dfRegister = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol},{'_id':0}))
        Date = Tansaction['date']
        dfRegister = dfRegister[dfRegister['date']>=Date]
        dfRegister = dfRegister.set_index('نام و نام خانوادگی')
        newVol = data['dataTrade']['volume'] - Tansaction['volume']
        DateList = list(set(dfRegister['date'].to_list()))
        for d in DateList:
            dfRegisterD = dfRegister[dfRegister['date']==d]
            balanceSaller = dfRegisterD['تعداد سهام'][data['dataTrade']['sell']] - newVol
            balanceBuyer = dfRegisterD['تعداد سهام'][data['dataTrade']['buy']] + newVol
            if balanceSaller<0 or balanceBuyer<0: return json.dumps({'replay':False,'msg':'ویرایش قابل اجرا نیست'})
            dfRegisterD['تعداد سهام'][data['dataTrade']['sell']] = balanceSaller
            dfRegisterD['تعداد سهام'][data['dataTrade']['buy']] = balanceBuyer
            dfRegister = dfRegister[dfRegister['date']!=d]
            dfRegister = pd.concat([dfRegister,dfRegisterD])
        for d in DateList:
            farasahmDb['registerNoBours'].delete_many({'symbol':symbol,'date':d})
        dfRegister = dfRegister.reset_index()
        dfRegister = dfRegister.to_dict('records')
        farasahmDb['registerNoBours'].insert_many(dfRegister)
        farasahmDb['transactions'].delete_many({'symbol':symbol,'id':int(data['dataTrade']['id'])})
        data['dataTrade']['date'] = Tansaction['date']
        farasahmDb['transactions'].insert_one(data['dataTrade'])
        return json.dumps({'replay':True})


def deltransaction(data):
    symbol = data['access'][1]
    dfRegister = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol},{'_id':0}))
    Date = data['transaction']['date']
    dfRegister = dfRegister[dfRegister['date']>=Date]
    dfRegister = dfRegister.set_index('نام و نام خانوادگی')
    DateList = list(set(dfRegister['date'].to_list()))
    for d in DateList:
        dfRegisterD = dfRegister[dfRegister['date']==d]
        balanceSaller = dfRegisterD['تعداد سهام'][data['transaction']['sell']] + data['transaction']['volume']
        balanceBuyer = dfRegisterD['تعداد سهام'][data['transaction']['buy']] - data['transaction']['volume']
        if balanceSaller<0 or balanceBuyer<0: return json.dumps({'replay':False,'msg':'حذف قابل اجرا نیست'})
        dfRegisterD['تعداد سهام'][data['transaction']['sell']] = balanceSaller
        dfRegisterD['تعداد سهام'][data['transaction']['buy']] = balanceBuyer
        dfRegister = dfRegister[dfRegister['date']!=d]
        dfRegister = pd.concat([dfRegister,dfRegisterD])
    for d in DateList:
        farasahmDb['registerNoBours'].delete_many({'symbol':symbol,'date':d})
    dfRegister = dfRegister.reset_index()
    dfRegister = dfRegister.to_dict('records')
    farasahmDb['registerNoBours'].insert_many(dfRegister)
    farasahmDb['transactions'].delete_many({'symbol':symbol,'id':int(data['transaction']['id'])})
    return json.dumps({'replay':True})


def addtradernobourse(data):
    symbol = data['access'][1]
    data['dataTrader']['نام و نام خانوادگی'] = data['dataTrader']['نام و نام خانوادگی'].replace('  ',' ').strip()
    if '_id' in data['dataTrader']:
        id_ = data['dataTrader']['_id']
        del data['dataTrader']['_id']
        befor = farasahmDb['registerNoBours'].find_one({'_id':ObjectId(id_)})
        if befor == None: return json.dumps({'replay':False,'msg':'سهامدار یافت نشد'})
        newAfter = data['dataTrader']
        farasahmDb['Priority'].update_many({"کد ملی": befor['کد ملی'], 'symbol': symbol}, {'$set': {'نام و نام خانوادگی': newAfter['نام و نام خانوادگی'], 'کد ملی': newAfter['کد ملی'], 'نام پدر': newAfter['نام پدر'], 'شماره تماس': newAfter['شماره تماس']}})
        farasahmDb['PriorityPay'].update_many({'frm':befor['نام و نام خانوادگی']},{'$set':{'frm':newAfter['نام و نام خانوادگی']}})
        farasahmDb['PriorityTransaction'].update_many({'frm':befor['نام و نام خانوادگی']},{'$set':{'frm':newAfter['نام و نام خانوادگی']}})
        farasahmDb['PriorityTransaction'].update_many({'to':befor['نام و نام خانوادگی']},{'$set':{'to':newAfter['نام و نام خانوادگی']}})
        farasahmDb['registerNoBours'].update_many({'_id':ObjectId(id_)},{'$set':data['dataTrader']})
    else:
        check = farasahmDb['registerNoBours'].find_one({'symbol':symbol,'نام و نام خانوادگی':data['dataTrader']['نام و نام خانوادگی']})!=None
        if check:return json.dumps({'replay':False,'msg':'سهامداری با همین نام موجود است امکان ثبت وجود ندارد'})
        check = farasahmDb['registerNoBours'].find_one({'symbol':symbol,'کد ملی':data['dataTrader']['کد ملی']})!=None
        if check:
            return json.dumps({'replay':False,'msg':'سهامداری با همین کد ملی موجود است امکان ثبت وجود ندارد'})
        lastDate = farasahmDb['registerNoBours'].find_one({'symbol':symbol},sort=[("date", pymongo.DESCENDING)])['date']
        dic = data['dataTrader']
        dic['symbol'] = symbol
        dic['date'] = lastDate
        dic['تعداد سهام'] = 0
        farasahmDb['registerNoBours'].insert_one(dic)
    return json.dumps({'replay':True})


def delshareholders(data):
    symbol = data['access'][1]
    name = data['transaction']['نام و نام خانوادگی']
    check = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol,'نام و نام خانوادگی':name}))
    if check['تعداد سهام'].max()>0: return json.dumps({'replay':False,'msg':f'"{name}" قابل حذف نیست'})
    trnc = farasahmDb['transactions'].find_one({'symbol':symbol,'sell':name})!=None
    if trnc: return json.dumps({'replay':False,'msg':f'"{name}" قابل حذف نیست'})
    trnc = farasahmDb['transactions'].find_one({'symbol':symbol,'buy':name})!=None
    if trnc: return json.dumps({'replay':False,'msg':f'"{name}" قابل حذف نیست'})
    farasahmDb['registerNoBours'].delete_many({'symbol':symbol,'نام و نام خانوادگی':name})
    return json.dumps({'replay':True})

def setinformationcompany(data):
    symbol = data['access'][1]
    dic = data['information']
    dic['symbol'] = symbol
    farasahmDb['companyBasicInformation'].delete_many({'symbol':symbol})
    farasahmDb['companyBasicInformation'].insert_one(dic)
    return json.dumps({'replay':True})

def syncBoursi(data):
    admin = adminCheck(data['id'])
    if admin:
        df = list(set(pd.DataFrame(farasahmDb['registerNoBours'].find())['کد ملی'].to_list()))
        for i in df:
            newData = farasahmDb['register'].find_one({'شناسه ملی':int(i)})
            if newData != None:
                farasahmDb['registerNoBours'].update_many({'کد ملی':i},{'$set':{'تاریخ تولد':newData['تاریخ تولد'],'کدبورسی':newData['کد سهامداری'],'صادره':newData['محل صدور'],'نام پدر':newData['نام پدر']}})
    return json.dumps({'replay':True})


def syncbook(data):
    admin = adminCheck(data['id'])
    if admin:
        df = pd.DataFrame(farasahmDb['registerNoBours'].find())
        symbols = list(set(df['symbol']))
    return json.dumps({'replay':True})

def createassembly(data):
    symbol = data['access'][1]
    date = data['date']/1000
    date = datetime.datetime.fromtimestamp(date)
    if date<= datetime.datetime.now():
        return json.dumps({'replay':False, 'msg': 'تاریخ نمیتواند ماقبل اکنون باشد'})
    dic = data['dict'].copy()
    dic['symbol'] = symbol
    dic['date'] = date
    dic['controller'] = data['controller']
    if '_id' in data['dict'].keys():
        del dic['_id']
        farasahmDb['assembly'].update_one({'_id':ObjectId(data['dict']['_id'])},{'$set':dic})
    else:
        farasahmDb['assembly'].insert_one(dic)
    return json.dumps({'replay':True})


def delassembly(data):
    symbol = data['access'][1]
    assembly = data['idassembly']
    farasahmDb['assembly'].delete_one({'_id':ObjectId(assembly['_id'])})
    return json.dumps({'replay':True})

def addpersonalassembly(data):
    symbol = data['access'][1]
    dic = data['dataPersonal']
    dic['symbol'] = symbol
    check = farasahmDb['personalAssembly'].find_one({'کد ملی':dic['کد ملی'],'symbol':symbol})
    if check != None:
        return json.dumps({'replay':False,'msg':'فرد قبلا به حاضرین اضافه شده'})
    farasahmDb['personalAssembly'].insert_one(dic)
    return json.dumps({'replay':True})

def delpersonalassembly(data):
    symbol = data['access'][1]
    check = farasahmDb['personalAssembly'].delete_one({'کد ملی':int(data['row']),'symbol':symbol})
    return json.dumps({'replay':True})



def addcapitalincrease(data):
    symbol = data['access'][1]
    company = farasahmDb['companyList'].find_one({'symbol':symbol})
    if company['type']=='NoBourse':
        date = datetime.datetime.fromtimestamp(data['dateSelection']/1000)
        date = int(str(JalaliDate.to_gregorian(date)).replace('-',''))
        df = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol}))
        #df = df[df['date']<=date]
        df = df[df['date']==df['date'].max()]
        df['تعداد سهام'] = df['تعداد سهام'].apply(int)
        df = df.drop_duplicates(subset=['کد ملی'])

        if len(df)==0:
            return json.dumps({'replay':False,'msg':'در تاریخ ذکر شده سهامداری یافت نشد'})
        if data['data']['methode'] == 'آورده سهامداران':
            total = df['تعداد سهام'].sum()
            grow = (int(data['data']['cuont'])/total)-1
            dff = df[['نام و نام خانوادگی','کد ملی','نام پدر','تعداد سهام','symbol','شماره تماس']]
            dff['حق تقدم'] = dff['تعداد سهام'] * grow
            dff['حق تقدم'] = dff['حق تقدم'].astype(int)
            dff['تاریخ'] = str(JalaliDate(datetime.datetime.fromtimestamp(int(data['dateSelection'])/1000))).replace('-','/')
            dff['dateInt'] = int(str(JalaliDate(datetime.datetime.fromtimestamp(int(data['dateSelection'])/1000))).replace('-',''))
            semi = int(data['data']['cuont']) - dff['حق تقدم'].sum() - total
            if 'پاره سهم' in dff['نام و نام خانوادگی'].to_list():
                dff.loc[df['نام و نام خانوادگی'] == 'پاره سهم', 'تعداد سهام'] = df[df['نام و نام خانوادگی'] == 'پاره سهم']['تعداد سهام'] + semi
            else:
                semiDf = pd.DataFrame([{'نام و نام خانوادگی':'پاره سهم',"کد ملی":"99","حق تقدم":semi,'نام پدر':''}])
                farasahmDb['registerNoBours'].insert_one({'نام و نام خانوادگی':'پاره سهم',"کد ملی":"99","تعداد سهام":0,'نام پدر':'','شماره تماس':'','symbol':symbol,'date':int(df['date'].max()),'rate':0,'صادره':''})
                dff = pd.concat([dff,semiDf])
            dff = dff.fillna(method='ffill').reset_index().drop(columns=['index'])
            dff['حق تقدم استفاده شده'] = 0
            dff['enable'] = True

            dff = dff.to_dict('records')
            farasahmDb['Priority'].insert_many(dff)
            dic = {'date':str(JalaliDate(datetime.datetime.fromtimestamp(int(data['dateSelection'])/1000))).replace('-','/'),
                   'newCount':int(data['data']['cuont']),'newCapitalIns':int(data['data']['capital']),'symbol':symbol,'methode':data['data']['methode'],'rate':grow*100, 'enable':True}
            farasahmDb['capitalIns'].insert_one(dic)
    return json.dumps({'replay':True})


def delcapitalincrease(data):
    symbol = data['access'][1]
    id = ObjectId(data['id'])
    dic = farasahmDb['capitalIns'].find_one({'_id':id})
    farasahmDb['Priority'].delete_many({'symbol':symbol,'تاریخ':dic['date']})
    farasahmDb['capitalIns'].delete_many({'_id':id})
    return json.dumps({'replay':True})



def settransactionpriority(data):
    symbol = data['access'][1]
    transaction = data['transaction']
    transaction['symbol'] = symbol
    transaction['date'] = datetime.datetime.now()
    transaction['capDate'] = data['datePriority']

    frmBalance = farasahmDb['Priority'].find_one({'symbol':symbol,'نام و نام خانوادگی':transaction['frm'], 'تاریخ':data['datePriority']})

    if frmBalance == None:
        return json.dumps({'replay':False,'msg':'فروشنده یافت نشد'})
    frmBalance = int(frmBalance['حق تقدم'])
    if frmBalance<int(transaction['count']) and transaction['frm']!='حق تقدم استفاده نشده':
        return json.dumps({'replay':False,'msg':'تعداد حق تقدم فروشنده کافی نیست'})
    
    frmBalance = frmBalance - int(transaction['count'])
    toBalance = farasahmDb['Priority'].find_one({'symbol':symbol,'نام و نام خانوادگی':transaction['to'], 'تاریخ':data['datePriority']})
    if toBalance == None:
        toBalance = farasahmDb['registerNoBours'].find_one({'symbol':symbol,'نام و نام خانوادگی':transaction['to']})
        if toBalance == None:
            return json.dumps({'replay':False,'msg':'خریدار یافت نشد'})
        PriorityDate = farasahmDb['Priority'].find_one({'symbol':symbol})
        farasahmDb['Priority'].update_one({'symbol':symbol,'نام و نام خانوادگی':transaction['frm'], 'تاریخ':data['datePriority']},{'$set':{'حق تقدم':frmBalance}})
        farasahmDb['Priority'].insert_one({'symbol':symbol,'نام و نام خانوادگی':transaction['to'],"کد ملی":toBalance['کد ملی'],'نام پدر':toBalance['نام پدر'],'تعداد سهام':0,'شماره تماس':toBalance['شماره تماس'],'حق تقدم':int(transaction['count']),'تاریخ':data['datePriority'] ,'dateInt':Fnc.dateSlashToInt(data['datePriority']),'حق تقدم استفاده شده':0})
        farasahmDb['PriorityTransaction'].insert_one(transaction)
        return json.dumps({'replay':True})
        
    toBalance = int(toBalance['حق تقدم'])
    toBalance = toBalance + int(transaction['count'])
    farasahmDb['Priority'].update_one({'symbol':symbol,'نام و نام خانوادگی':transaction['frm'], 'تاریخ':data['datePriority']},{'$set':{'حق تقدم':frmBalance}})
    farasahmDb['Priority'].update_one({'symbol':symbol,'نام و نام خانوادگی':transaction['to'], 'تاریخ':data['datePriority']},{'$set':{'حق تقدم':toBalance}})
    transaction['date'] = datetime.datetime.now()
    transaction['symbol'] = symbol
    farasahmDb['PriorityTransaction'].insert_one(transaction)
    return json.dumps({'replay':True})


def setpayprority(data):
    symbol = data['access'][1]
    pay = data['pay']
    pay['capDate'] = data['datePriority']

    frmBalance = farasahmDb['Priority'].find_one({'symbol':symbol, 'نام و نام خانوادگی':pay['frm'], 'تاریخ':data['datePriority']})
    if frmBalance == None:
        return json.dumps({'replay':False,'msg':'سهامدار یافت نشد'})
    if int(frmBalance['حق تقدم'])<int(pay['count']):
        return json.dumps({'replay':False,'msg':'تعداد حق تقدم سهامدار کافی نیست'})
    frmBalanceAfter = int(frmBalance['حق تقدم']) - int(pay['count'])
    count = int(pay['count']) + int(frmBalance['حق تقدم استفاده شده'])
    farasahmDb['Priority'].update_one({'symbol':symbol,'نام و نام خانوادگی':pay['frm'], 'تاریخ':data['datePriority']},{'$set':{'حق تقدم':frmBalanceAfter,'حق تقدم استفاده شده':count}})
    pay['date'] = str(JalaliDate.to_jalali(datetime.datetime.fromtimestamp(int(data['date'])/1000))).replace('-','/')
    pay['symbol'] = symbol
    pay['value'] = int(pay['value'])
    farasahmDb['PriorityPay'].insert_one(pay)
    return json.dumps({'replay':True})


def getprioritypay(data):
    symbol = data['access'][1]
    df = pd.DataFrame(farasahmDb['PriorityPay'].find({'symbol':symbol, 'capDate':data['date']},{'_id':0,'symbol':0}))
    if len(df) ==0:
         return json.dumps({'replay':False,'msg':'یافت نشد'})
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})

def delprioritypay(data):
    symbol = data['access'][1]
    data = data['dt']
    cheack = farasahmDb['PriorityPay'].find_one(data)
    if cheack == None: return json.dumps({'replay':False,'msg':'یافت نشد'})
    farasahmDb['PriorityPay'].delete_one({'_id':cheack['_id']})
    CheackBase = farasahmDb['Priority'].find_one({'نام و نام خانوادگی':cheack['frm']})
    if CheackBase == None: return json.dumps({'replay':False,'msg':'یافت نشد'})
    farasahmDb['Priority'].update_one({'نام و نام خانوادگی':cheack['frm']},{'$set':{'حق تقدم استفاده شده':int(CheackBase['حق تقدم استفاده شده'])-int(cheack['count']),'حق تقدم':int(CheackBase['حق تقدم'])+int(cheack['count'])}})
    return json.dumps({'replay':True})

def delprioritytransaction(data):
    doc = farasahmDb['PriorityTransaction'].find_one({'_id':ObjectId(data['id'])})
    if doc == None:
         return json.dumps({'replay':False,'msg':'یافت نشد'})
    newTo = farasahmDb['Priority'].find_one({"نام و نام خانوادگی":doc['to']})
    newTo['حق تقدم'] = int(newTo['حق تقدم']) - int(doc['count'])
    if newTo['حق تقدم'] < 0 and doc['to'] != 'حق تقدم استفاده نشده':
         return json.dumps({'replay':False,'msg':f'حذف انجام نشد، مانده دریافت کننده منفی میشود'})
    newFrm = farasahmDb['Priority'].find_one({"نام و نام خانوادگی":doc['frm']})
    newFrm['حق تقدم'] = int(newFrm['حق تقدم']) + int(doc['count'])
    farasahmDb['Priority'].update_one({"_id":newFrm['_id']},{"$set":{'حق تقدم':newFrm['حق تقدم']}})
    farasahmDb['Priority'].update_one({"_id":newTo['_id']},{"$set":{'حق تقدم':newTo['حق تقدم']}})
    farasahmDb['PriorityTransaction'].delete_one({'_id':ObjectId(data['id'])})
    return json.dumps({'replay':True})


@Fnc.retry_decorator(max_retries=3, sleep_duration=5)
def desk_broker_volumeTrade_cal():
    print('start cal volume trade')
    listDate = farasahmDb['TradeListBroker'].distinct('dateInt')
    for date in listDate:
        print('volume trade', date)
        df = pd.DataFrame(farasahmDb['TradeListBroker'].find({'dateInt':date},{'صندوق':1,'InstrumentCategory':1,'NetPrice':1,'_id':0}))
        df['NetPrice'] = df['NetPrice'].apply(int)
        df['صندوق'] = df['صندوق'].fillna(False)
        df['InstrumentCategory'] = df['InstrumentCategory'].replace('false',False).replace('true',True)
        df = df.groupby(['صندوق','InstrumentCategory']).sum().reset_index()
        dic = {}
        for i in df.index:
            if df['InstrumentCategory'][i]:
                dic['اوراق کارگزاری']  = int(df['NetPrice'][i])
            if df['صندوق'][i]:
                dic['صندوق کارگزاری']  = int(df['NetPrice'][i])
            else:
                dic['سهام کارگزاری']  = int(df['NetPrice'][i])
        dic['کل کارگزاری'] = int(df['NetPrice'].sum())

        tse = pd.DataFrame(farasahmDb['tse'].find({'dataInt':date},{'InstrumentCategory':1,'صندوق':1,'ارزش':1,'_id':0}))
        if len(tse) > 0:
            tse['ارزش'] = tse['ارزش'].apply(int)
            tse = tse.groupby(['صندوق','InstrumentCategory']).sum().reset_index()
            for i in tse.index:
                if tse['InstrumentCategory'][i]:
                    dic['اوراق بازار']  = int(tse['ارزش'][i])
                if tse['صندوق'][i]:
                    dic['صندوق بازار']  = int(tse['ارزش'][i])
                else:
                    dic['سهام بازار']  = int(tse['ارزش'][i])
            dic['کل بازار'] = int(tse['ارزش'].sum())
            dic['date'] = date
            for i in ['اوراق کارگزاری','صندوق کارگزاری','سهام کارگزاری','کل کارگزاری','کل بازار','اوراق بازار','صندوق بازار','سهام بازار']:
                if i not in dic.keys():
                    dic[i] = 0
            dic['نسبت کل'] = round(float(dic['کل کارگزاری'] / dic['کل بازار'])*100,2)
            dic['نسبت سهام'] = round(float(dic['سهام کارگزاری'] / dic['سهام بازار'])*100, 2)
            dic['نسبت صندوق'] = round(float(dic['صندوق کارگزاری'] / dic['صندوق بازار'])*100, 2)
            dic['نسبت اوراق'] = round(float(dic['اوراق کارگزاری'] / dic['اوراق بازار'])*100, 2)
            farasahmDb['deskBrokerVolumeTrade'].delete_many({'date':date})
            farasahmDb['deskBrokerVolumeTrade'].insert_one(dic)


'''
@Fnc.retry_decorator(max_retries=3, sleep_duration=5)
def desk_broker_volumeTrade_cal():
    print('start cal volume trade')
    pipeline = [{"$group":{ "_id": {"date": "$dateInt","type": "$InstrumentCategory","fund":"$صندوق"},"totalNetPrice": {"$sum": "$NetPrice"}}}]
    pipelineTse = [{"$match": {"dataInt": {"$gte": 14020101}}},{"$group":{ "_id": {"fund": "$صندوق","date": "$dataInt","InstrumentCategory":"$InstrumentCategory"},"totalNetPrice": {"$sum": "$ارزش"}}}]
    df = list(farasahmDb['TradeListBroker'].aggregate(pipeline))
    df = [{ 'date':x['_id']['date'] , 'type':x['_id']['type'] , 'fund':x['_id']['fund'] , 'value':x['totalNetPrice'] } for x in df]
    df = pd.DataFrame(df).sort_values(by=['date'])
    if len(df)==0:
        return json.dumps({'replay':False,'msg':'گزارشی یافت نشد'})
    df_oragh = df[df['type']=='true'][['date','value']].rename(columns={'value':'اوراق'}).set_index('date')
    df = df[df['type']=='false']
    df_fund = df[df['fund']==True][['date','value']].rename(columns={'value':'صندوق'}).set_index('date')
    df_stock = df[df['fund']==False][['date','value']].rename(columns={'value':'سهام'}).set_index('date')
    df = df_stock.join(df_fund,how='outer').join(df_oragh,how='outer').fillna(0)
    tse = list(farasahmDb['tse'].aggregate(pipelineTse))
    tse = [{ 'date':x['_id']['date'] , 'type':x['_id']['InstrumentCategory'] , 'fund':x['_id']['fund'] , 'value':x['totalNetPrice'] } for x in tse]
    tse = pd.DataFrame(tse)
    tse_oragh = tse[tse['type']==True][['date','value']].rename(columns={'value':'کل اوراق'}).set_index('date')
    tse = tse[tse['type']==False]
    tse_fund = tse[tse['fund']==True][['date','value']].rename(columns={'value':'کل صندوق ها'}).set_index('date')
    tse_stoke = tse[tse['fund']==False][['date','value']].rename(columns={'value':'کل سهام'}).set_index('date')
    df = df.join(tse_oragh).join(tse_fund).join(tse_stoke)
    df['کل'] = df['سهام'] + df['اوراق'] + df['صندوق']
    df['کل بازار'] = df['کل سهام'] + df['کل صندوق ها'] + df['کل اوراق']
    df['نسبت کل'] = df['کل'] / df['کل بازار']
    df['نسبت اوراق'] = df['اوراق'] / df['کل اوراق']
    df['نسبت صندوق'] = df['صندوق'] / df['کل صندوق ها']
    df['نسبت سهام'] = df['سهام'] / df['کل سهام']
    df = df.reset_index()
    df = df.to_dict('records')
    farasahmDb['deskBrokerVolumeTrade'].delete_many({})
    farasahmDb['deskBrokerVolumeTrade'].insert_many(df)
'''

@Fnc.retry_decorator(max_retries=3, sleep_duration=5)
def desk_broker_turnover_cal():
    print('get turnover in broker')
    pipeline = [{"$group":{ "_id": {"date": "$dateInt","type": "$InstrumentCategory","fund":"$صندوق"},"totalNetPrice": {"$sum": "$NetPrice"}}}]
    df = list(farasahmDb['TradeListBroker'].aggregate(pipeline))
    df = [{ 'date':x['_id']['date'] , 'type':x['_id']['type'] , 'fund':x['_id']['fund'] , 'value':x['totalNetPrice'] } for x in df]
    df = pd.DataFrame(df).sort_values(by=['date'])
    if len(df)==0:
        return json.dumps({'replay':False,'msg':'گزارشی یافت نشد'})
    df_oragh = df[df['type']=='true'][['date','value']].rename(columns={'value':'اوراق'}).set_index('date')
    df = df[df['type']=='false']
    df_fund = df[df['fund']==True][['date','value']].rename(columns={'value':'صندوق'}).set_index('date')
    df_stock = df[df['fund']==False][['date','value']].rename(columns={'value':'سهام'}).set_index('date')
    df = df_stock.join(df_fund,how='outer').join(df_oragh,how='outer').fillna(0)
    sabadCode = farasahmDb['codeTraderInSabad'].distinct('code')
    pipeline = [{"$match": {"TradeCode": {"$in": sabadCode}}},{"$group":{ "_id": {"date": "$dateInt","type": "$InstrumentCategory","fund":"$صندوق"},"totalNetPrice": {"$sum": "$NetPrice"}}}]
    sabad = list(farasahmDb['TradeListBroker'].aggregate(pipeline))
    sabad = [{ 'date':x['_id']['date'] , 'type':x['_id']['type'] , 'fund':x['_id']['fund'] , 'value':x['totalNetPrice'] } for x in sabad]
    sabad = pd.DataFrame(sabad).sort_values(by=['date'])
    if len(sabad)==0:
        return json.dumps({'replay':False,'msg':'گزارشی یافت نشد'})
    sabad_oragh = sabad[sabad['type']=='true'][['date','value']].rename(columns={'value':'اوراق سبد'}).set_index('date')
    sabad_fund = sabad[sabad['fund']==True][['date','value']].rename(columns={'value':'صندوق سبد'}).set_index('date')
    sabad = sabad[sabad['type']=='false']
    sabad_stock = sabad[sabad['fund']==False][['date','value']].rename(columns={'value':'سهام سبد'}).set_index('date')
    sabad = sabad_stock.join(sabad_fund,how='outer').join(sabad_oragh,how='outer').fillna(0)
    df = df.join(sabad_oragh).join(sabad_fund).join(sabad_stock)

    df['کل'] = df['سهام'] + df['اوراق'] + df['صندوق']
    df['سبد'] = df['اوراق سبد'] + df['صندوق سبد'] + df['سهام سبد']
    df['نسبت کل'] = df['سبد'] / df['کل']
    df['نسبت اوراق'] = df['اوراق سبد'] / df['اوراق']
    df['نسبت صندوق'] = df['صندوق سبد'] / df['صندوق']
    df['نسبت سهام'] = df['سهام سبد'] / df['سهام']
    df = df.reset_index()
    df = df.to_dict('records')
    farasahmDb['deskSabadTurnover'].delete_many({})
    farasahmDb['deskSabadTurnover'].insert_many(df)


def getdatepriority(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    df = pd.DataFrame(farasahmDb['capitalIns'].find({'symbol':symbol},{'_id':0}))
    if len(df) == 0:
        return json.dumps({'reply':False,'msg':'رویدداد افزایش سرمایه یافت نشد'})
    df = df.to_dict('records')
    return json.dumps({'reply':True,'lst':df})



def setnewbankbalance(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    dic = data['bank']
    startDate = data['startDate']/1000
    startDate = datetime.datetime.fromtimestamp(startDate)
    dic['startDate'] = startDate
    dic['symbol'] = symbol
    farasahmDb['bankBalance'].insert_one(dic)
    return json.dumps({'reply':True})



def delbankassetfund(data):
    access = data['access'][0]
    symbol = data['access'][1]
    symbol = farasahmDb['menu'].find_one({'name':symbol})['symbol']
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    row = data['row']
    if row['type'] != 'سپرده بانکی':
        return json.dumps({'reply':False,'msg':'این نوع دارایی قابل حذف نیست'})
    farasahmDb['bankBalance'].delete_many({'name':row['name'],'num':row['num']})
    return json.dumps({'reply':True})
