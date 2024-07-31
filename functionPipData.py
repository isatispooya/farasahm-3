import pandas as pd
from setting import farasahmDb
import Fnc
from ApiMethods import GetDailyTradeList, GetCustomerMomentaryAssets, GetCustomerRemainWithTradeCode, GetCustomerByNationalCode,GetFirmByNationalIdentification
import datetime
from persiantools.jdatetime import JalaliDate
import requests
from persiantools import characters


def todayIntJalali():
    today = datetime.datetime.now()
    jalali = JalaliDate.to_jalali(today)
    jalali = int(str(jalali).replace('-',''))
    return jalali

def drop_duplicet_TradeListBroker():
    jalaliInt = todayIntJalali()
    df = pd.DataFrame(farasahmDb['TradeListBroker'].find({"dateInt":jalaliInt},{'_id':0}))
    df = df.drop_duplicates(subset=['NetPrice','Price','TotalCommission','TradeCode','dateInt','TradeNumber','TradeSymbol','TradeType','Volume'])
    if len(df)>0:
        farasahmDb['TradeListBroker'].delete_many({"dateInt":jalaliInt})
        farasahmDb['TradeListBroker'].insert_many(df.to_dict('records'))
        
        
        
def GetAllTradeToDay():
    '''
    دریافت همه معاملات کارگزاری در روز جاری
    '''
    DateInt = Fnc.todayIntJalali()
    doDay = Fnc.toDayJalaliListYMD()
    page = 1
    while True:
        print(doDay[0],doDay[1],doDay[2],page,'broker start')
        symbolList = farasahmDb['tse'].find({},{'نام':1,'نماد':1,'_id':0,'صندوق':1})
        symbolList = pd.DataFrame(symbolList)
        symbolList = symbolList.drop_duplicates()
        symbolList['نماد'] = symbolList['نماد'].apply(Fnc.remove_non_alphanumeric)
        df = GetDailyTradeList(doDay[0], doDay[1] ,doDay[2],page,1000)
        if len(df) == 0:
            break
        df = pd.DataFrame(df)
        df['DateYrInt'] = doDay[0]
        df['DateMnInt'] = doDay[1]
        df['DateDyInt'] = doDay[2]
        df['dateInt'] = df['TradeDate'].apply(Fnc.dateStrToIntJalali)
        df['page'] = page
        for i in ['NetPrice','Price','TotalCommission','Volume']:
            df[i] = df[i].apply(int)
        df['Update'] = datetime.datetime.now()
        df['TradeSymbolAbs'] = df['TradeSymbol'].apply(Fnc.remove_non_alphanumeric)
        df = df.set_index('TradeSymbolAbs').join(symbolList.set_index('نماد'))
        df = df.reset_index()
        print(doDay[0],doDay[1],doDay[2],page,'broker end')
        df = df.to_dict('records')
        farasahmDb['TradeListBroker'].insert_many(df)
        drop_duplicet_TradeListBroker()
        page = page + 1

def get_asset_customer():
    '''
    دریافت دارایی دارندگان نماد های مشخص شده
    '''
    symbols = ['ویسا','بازرگام','خاتم']
    for symbol in symbols:
        today = farasahmDb['TradeListBroker'].distinct('dateInt')
        today = max(today)
        df = farasahmDb['TradeListBroker'].find({"dateInt":today,"TradeSymbol":symbol+'1'})
        df = pd.DataFrame(df)
        if len(df)>0:
            df = df.drop_duplicates(subset='TradeCode')
            TradeCodes = df['TradeCode'].to_list()
            for TradeCode in TradeCodes:
                assets = pd.DataFrame(GetCustomerMomentaryAssets(TradeCode))
                if len(assets)>0:
                    assets['TradeCode'] = TradeCode
                    assets['dateInt'] = today
                    assets['update'] = datetime.datetime.now()
                    assets = assets.to_dict('records')
                    farasahmDb['assetsCoustomerBroker'].delete_many({"TradeCode":TradeCode})
                    farasahmDb['assetsCoustomerBroker'].insert_many(assets)
                    



def GetAllTradeLast30Day():
    '''
    دریافت همه معاملات کارگزاری در 30 روز گذشته
    '''
    today = datetime.datetime.now()
    for d in range(1,30):
        date = today - datetime.timedelta(days=d)
        dateInt = Fnc.gorgianIntToJalaliInt(date)
        doDay = JalaliDate.to_jalali(date)
        doDay = str(doDay).split('-')
        doDay = [int(x) for x in doDay]
        page = 1
        while True:
            symbolList = farasahmDb['tse'].find({},{'نام':1,'نماد':1,'_id':0,'صندوق':1})
            symbolList = pd.DataFrame(symbolList)
            symbolList = symbolList.drop_duplicates()
            symbolList['نماد'] = symbolList['نماد'].apply(Fnc.remove_non_alphanumeric)
            df = GetDailyTradeList(doDay[0], doDay[1] ,doDay[2],page,1000)
            if len(df) == 0:
                break
            df = pd.DataFrame(df)
            df['DateYrInt'] = doDay[0]
            df['DateMnInt'] = doDay[1]
            df['DateDyInt'] = doDay[2]
            df['dateInt'] = df['TradeDate'].apply(Fnc.dateStrToIntJalali)
            df['page'] = page
            for i in ['NetPrice','Price','TotalCommission','Volume']:
                df[i] = df[i].apply(int)
            df['Update'] = datetime.datetime.now()
            df['TradeSymbolAbs'] = df['TradeSymbol'].apply(Fnc.remove_non_alphanumeric)
            df = df.set_index('TradeSymbolAbs').join(symbolList.set_index('نماد'))
            df = df.reset_index()
            print(doDay[0],doDay[1],doDay[2],page,'broker end')
            df = df.to_dict('records')
            farasahmDb['TradeListBroker'].insert_many(df)
            drop_duplicet_TradeListBroker()
            page = page + 1
            

def TseRepir():
    '''
    دریافت اطلاعات Tse برای 30 روز گذشته
    '''
    dateList = farasahmDb['TradeListBroker'].distinct('dateInt')
    for date in dateList:
        jalaliStr = str(date)
        jalaliStr = jalaliStr[:4]+'/'+jalaliStr[4:6]+'/'+jalaliStr[6:]
        res = requests.get(url=f'http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d={date}')
        if res.status_code == 200:
            df = pd.read_excel(res.content,header=2, engine='openpyxl')
            if len(df)>10:
                df['نماد'] = df['نماد'].apply(characters.ar_to_fa)
                df['نام'] = df['نام'].apply(characters.ar_to_fa)
                df['صندوق'] = df['نام'].apply(Fnc.isFund)
                df['InstrumentCategory'] = df['نام'].apply(Fnc.isOragh)
                df['data'] = jalaliStr
                df['dataInt'] = date
                df['timestump'] = 0
                df['time'] = str(15) +':'+str('00')+':'+str('00')
                df = df.to_dict('records')
                farasahmDb['tse'].delete_many({'dataInt':date})
                farasahmDb['tse'].insert_many(df)



def CuostomerRemain():
    '''
    دریافت مانده حساب مشتریان در کارگزاری
    
    '''
    listCode_all = farasahmDb['TradeListBroker'].distinct('TradeCode')
    for i in range(0,len(listCode_all)):
        code = listCode_all[i]
        try:
            result = GetCustomerRemainWithTradeCode(str(code),'TSE')
            result['datetime'] = datetime.datetime.now()
            result['code'] = code
            tradelist = farasahmDb['TradeListBroker'].find_one({'TradeCode':str(code)},sort=[('dateInt',-1)])
            result['lastTradeDateGor'] = tradelist['TradeDate']
            result['lastTradeDateJal'] = tradelist['dateInt']
            result['Branch'] = tradelist['BranchTitle']
            farasahmDb['CustomerRemain'].delete_many({'TradeCode':str(code)})
            farasahmDb['CustomerRemain'].insert_one(result)
        except:
            pass


def desk_broker_volumeTrade_cal():
    '''
    محاسبه گر حجم معاملات کارگزاری در داشبرد
    '''
    listDate = farasahmDb['TradeListBroker'].distinct('dateInt')
    for date in listDate:
        print('desk_broker_volumeTrade_cal' , date)
        df = pd.DataFrame(farasahmDb['TradeListBroker'].find({'dateInt':date},{'صندوق':1,'InstrumentCategory':1,'NetPrice':1,'_id':0,'TradeCode':1,'TradeDate':1,'TradeNumber':1,'TradeSymbol':1}))
        df = df.drop_duplicates()
        df = df[['صندوق','InstrumentCategory','NetPrice']]
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

def task_desk_broker_turnover_cal():
    '''
    محاسبه گر اطلاعات گردش مالی سبدگردانی در داشبرد
    '''
    pipeline = [{"$group":{ "_id": {"date": "$dateInt","type": "$InstrumentCategory","fund":"$صندوق"},"totalNetPrice": {"$sum": "$NetPrice"}}}]
    df = list(farasahmDb['TradeListBroker'].aggregate(pipeline))
    df = [{ 'date':x['_id']['date'] , 'type':x['_id']['type'] , 'fund':x['_id']['fund'] , 'value':x['totalNetPrice'] } for x in df]
    df = pd.DataFrame(df).sort_values(by=['date'])
    print('task_desk_broker_turnover_cal',0)
    
    if len(df)>0:
        
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
        print('task_desk_broker_turnover_cal',1)
        if len(sabad)==0:
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



def get_asset_funds():
    '''
    دریافت دارایی های صندوق ها از کارگزاری
    '''
    lst_code = [{'code':'61580209324','symbol':'خاتم'}]
    date = Fnc.todayIntJalali()
    for i in lst_code:
        df = pd.DataFrame(GetCustomerMomentaryAssets(i['code']))
        if len(df)>0:
            df['VolumeInPrice'] = df['VolumeInPrice'].apply(int)
            df['Volume'] = df['Volume'].apply(int)
            df['Price'] = df['VolumeInPrice'] / df['Volume']
            df['Fund'] = i['symbol']
            df['date'] = date
            df = df.drop(columns=['CustomerTitle','TradeCode','TradeSystemId'])
            df['type'] = df['Symbol'].apply(Fnc.setTypeInFundBySymbol)
            df = df.to_dict('records')
            farasahmDb['assetFunds'].delete_many({'Fund':i['symbol'],'date':date})
            farasahmDb['assetFunds'].insert_many(df)



def getAssetCoustomerByFixincome():
    '''
    دریافت اطلاعات افرادی که اوراق با دارامد ثابت دارند
    '''
    conditions = {'$or': [{'صندوق': True}, {'InstrumentCategory': 'true'}]}
    codelist = pd.DataFrame(farasahmDb['TradeListBroker'].find(conditions,{'_id':0,'dateInt':1,'TradeCode':1}))
    print('getAssetCoustomerByFixincome')
    if len(codelist):
        codelist = list(set(codelist[codelist['dateInt']==codelist['dateInt'].max()]['TradeCode']))
        today = datetime.datetime.now()
        dateInt = Fnc.gorgianIntToJalaliInt(today)
        for i in codelist:
            asset = pd.DataFrame(GetCustomerMomentaryAssets(i))
            if len(asset)>0:
                asset['datetime'] = today
                asset['dateInt'] = dateInt
                asset = asset.to_dict('records')
                farasahmDb['assetCoustomerOwnerFix'].delete_many({'TradeCode':i, 'dateInt':dateInt})
                farasahmDb['assetCoustomerOwnerFix'].insert_many(asset)


def getTseToDay():
    date=datetime.datetime.now()
    dt = datetime.datetime(date.year,date.month,date.day,15,0,0)
    jalali = JalaliDate.to_jalali(date)
    jalaliStr = str(jalali).replace('-','/')
    jalaliInt =int(str(jalali).replace('-',''))
    avalibale = farasahmDb['tse'].find_one({'dataInt':jalaliInt})
    if (date != datetime.datetime.now() and avalibale!=None) == False:
        if date != datetime.datetime.now():
            res = requests.get(url=f'http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d={jalali}')
        else:
            res = requests.get(url='http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d=0')
        if res.status_code == 200:
            df = pd.read_excel(res.content,header=2, engine='openpyxl')
            if len(df)>10:
                df['نماد'] = df['نماد'].apply(characters.ar_to_fa)
                df['نام'] = df['نام'].apply(characters.ar_to_fa)
                df['صندوق'] = df['نام'].apply(Fnc.isFund)
                df['InstrumentCategory'] = df['نام'].apply(Fnc.isOragh)
                df['data'] = jalaliStr
                df['dataInt'] = jalaliInt
                df['timestump'] = dt.timestamp()
                df['navAmary'] = 0
                df['countunit'] = 0
                df['time'] = str(dt.hour) +':'+str(dt.minute)+':'+str(dt.second)
                df = df.to_dict('records')
                farasahmDb['tse'].delete_many({'dataInt':jalaliInt})
                farasahmDb['tse'].insert_many(df)


def get_trade_code () :
    date_trader_list = farasahmDb ['TradeListBroker'].distinct('dateInt')
    for i in date_trader_list :
        trade_list = farasahmDb['TradeListBroker'].find({'dateInt': i} ,{'_id' :0 , 'TradeCode' :1})
        trade_list = [x['TradeCode'] for x in trade_list]
        trade_list = set(trade_list)
        customer_list = set(farasahmDb['customerofbroker'].distinct('TradeCode'))

        need_update = list(trade_list-customer_list)
        for j in need_update:
            nc = str(j)[4:]
            customer = GetCustomerByNationalCode(nc)
            if len(customer)>0:
                farasahmDb['customerofbroker'].insert_one(customer)




