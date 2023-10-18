from persiantools.jdatetime import JalaliDate
import datetime
import pandas as pd
import requests
from persiantools.jdatetime import JalaliDate
from persiantools import characters, digits
import pymongo
from pymongo import ASCENDING,DESCENDING
import time
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

def gorgianIntToJalaliInt(date):
    date = str(date).replace('-','')
    y = str(date)[:4]
    m = str(date)[4:6]
    d = str(date)[6:8]

    Jalali = JalaliDate.to_jalali(int(y),int(m),int(d))
    return int(str(Jalali).replace('-',''))

def JalalistrToGorgia(date):
    date = str(date).replace('-','')
    y = int(str(date)[:4])
    m = int(str(date)[4:6])
    d = int(str(date)[6:8])
    return JalaliDate(y,m,d).to_gregorian()

def gorgianIntToJalali(date):
    date = str(date).replace('-','')
    y = str(date)[:4]
    m = str(date)[4:6]
    d = str(date)[6:8]

    Jalali = JalaliDate.to_jalali(int(y),int(m),int(d))
    return str(Jalali)


def todayIntJalali():
    today = datetime.datetime.now()
    jalali = JalaliDate.to_jalali(today)
    jalali = int(str(jalali).replace('-',''))
    return jalali

def element_to_dict(element):
    result = {}
    if element.attrib:
        result.update(element.attrib)
    for child in element:
        child_dict = element_to_dict(child)
        if child.tag in result:
            if not isinstance(result[child.tag], list):
                result[child.tag] = [result[child.tag]]
            result[child.tag].append(child_dict)
        else:
            result[child.tag] = child_dict
    if element.text:
        result["text"] = element.text
    return result


def GenerateDatetime(yr,mn,dy,hr,mi):
    fromJalali = JalaliDate(yr, mn, dy).to_gregorian()
    fromJalali = str(fromJalali).split('-')
    fromJalali = [int(x) for x in fromJalali]
    fromDate = datetime.datetime(fromJalali[0],fromJalali[1],fromJalali[2],int(hr),int(mi))
    return fromDate.strftime("%Y-%m-%dT%H:%M:%S")

def GenerateDate(yr,mn,dy):
    fromDate = JalaliDate(yr, mn, dy).to_gregorian()
    return fromDate.strftime("%Y-%m-%d")

def nDayLastDate(date,nDay):
    fromDate = datetime.datetime.strptime(date,"%Y-%m-%d") - datetime.timedelta(days=nDay)
    return fromDate.strftime("%Y-%m-%d")

def dateIntJalaliToGorgian(x):
    x =str(x)
    x = GenerateDatetime(int(x[:4]),int(x[4:6]),int(x[6:8]),0,0)
    return x


def is_time_between(start,end):
    now = datetime.datetime.now()
    current_hour = now.hour
    if start <= current_hour <= end:
        return True
    else:
        return False
    

def toDayJalaliListYMD(Today = datetime.datetime.now()):
    Today = JalaliDate.to_jalali(Today)
    Today = str(Today).split('-')
    Today = [int(x) for x in Today]
    return Today



def is_time_divisible(x):
    now = datetime.datetime.now()
    current_minute = now.minute
    if current_minute % x == 0:
        return True
    else:
        return False
    


def timestumpToJalalInt(date):
    date = int(date)/1000
    date = datetime.datetime.fromtimestamp(date)
    date = JalaliDate(date)
    date = str(date).replace('-','')
    date = int(date)
    return date



def dateStrToIntJalali(date):
    dateInt = datetime.datetime.strptime(date,"%Y-%m-%dT%H:%M:%S")
    dateInt = JalaliDate(dateInt)
    dateInt = str(dateInt).replace('-','')
    dateInt = int(dateInt)
    return dateInt

def isFund(x):
    if 'صندوق' in x:
        return True
    else:
        return False

def isOragh(name):
    lst = ['صكوك','صکوک','اجاره','اجاره','مرابحه','مرابحه','گام','گام','گواهي اعتبار مولد','گواهی اعتبار مولد','تامين مالي','تامین مالی','اسنادخزانه','اسنادخزانه''خريد دين','خرید دین','منفعت','منفعت','مشاركت','مشارکت','امتيازتسهيلات مسكن','امتیازتسهیلات مسکن']
    for l in lst:
        if (l in name):
            return True
    else:
        return False
    
def getTseDate(date=datetime.datetime.now()):
    dt = datetime.datetime(date.year,date.month,date.day,15,0,0)
    jalali = JalaliDate.to_jalali(date)
    jalaliStr = str(jalali).replace('-','/')
    jalaliInt =int(str(jalali).replace('-',''))
    avalibale = farasahmDb['tse'].find_one({'dataInt':jalaliInt})
    print('start get tse in', jalaliInt)
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
                df['صندوق'] = df['نام'].apply(isFund)
                df['InstrumentCategory'] = df['نام'].apply(isOragh)
                df['data'] = jalaliStr
                df['dataInt'] = jalaliInt
                df['timestump'] = dt.timestamp()
                df['time'] = str(dt.hour) +':'+str(dt.minute)+':'+str(dt.second)
                df = df.to_dict('records')
                farasahmDb['tse'].delete_many({'dataInt':jalaliInt})
                farasahmDb['tse'].insert_many(df)

def getTse30LastDay():
    for d in range(1,30):
        day = datetime.datetime.now() - datetime.timedelta(days=d)
        getTseDate(day)
        

def remove_non_alphanumeric(input_string):
    result = ''.join(character for character in input_string if character.isalpha())
    return result


def floatTo2decimal(x):
    x = x *100
    x = round(x,2)
    return x

def floatTo2decimalNormal(x):
    x = round(x,2)
    return x

def toBillionRial(x):
    x = x /1000000000
    x = int(x)
    return x




    

def Apply_Trade_Symbol(group,symbol,date):
    group['NetPrice_Buy'] = group[group['TradeType']=='Buy']['NetPrice'].sum()
    group['Volume_Buy'] = group[group['TradeType']=='Buy']['Volume'].sum()
    group['Price_Buy'] = group['NetPrice_Buy'] / group['Volume_Buy']
    group['TotalCommission_Buy'] = group[group['TradeType']=='Buy']['TotalCommission'].sum()
    group['NetPrice_Sell'] = group[group['TradeType']=='Sell']['NetPrice'].sum()
    group['Volume_Sell'] = group[group['TradeType']=='Sell']['Volume'].sum()
    group['Price_Sell'] = group['NetPrice_Sell'] / group['Volume_Sell']
    group['TotalCommission_Sell'] = group[group['TradeType']=='Sell']['TotalCommission'].sum()
    tree = group[['Price','Volume','TradeDate','BranchTitle','TradeType','NetPrice','TotalCommission']]
    tree['NetPrice_Buy'] = tree[tree['TradeType']=='Buy']['NetPrice']
    tree['Volume_Buy'] = tree[tree['TradeType']=='Buy']['Volume']
    tree['Price_Buy'] = tree[tree['TradeType']=='Buy']['Price']
    tree['TotalCommission_Buy'] = tree[tree['TradeType']=='Buy']['TotalCommission']
    tree['NetPrice_Sell'] = tree[tree['TradeType']=='Sell']['NetPrice']
    tree['Volume_Sell'] = tree[tree['TradeType']=='Sell']['Volume']
    tree['Price_Sell'] = tree[tree['TradeType']=='Sell']['Price']
    tree['TotalCommission_Sell'] = tree[tree['TradeType']=='Sell']['TotalCommission']
    tree = tree.drop(columns=['Price','Volume','TradeType','NetPrice','TotalCommission']).fillna(0)
    tree['TradeDate'] = [str(x).split('T')[1] for x in tree['TradeDate']]
    tree['balance'] = ' '
    tree = tree.to_dict('records')
    group = group.drop(columns=['NetPrice','Price','TotalCommission','TradeDate','TradeStationType','TradeType','TransferTax','Volume'])
    group = group[group.index==group.index.max()]
    group['_children'] = ''
    group['_children'][group.index.max()] = tree
    group = group.fillna(0)
    group['Price_Buy'] = group['Price_Buy'].apply(int)
    group['Price_Sell'] = group['Price_Sell'].apply(int)
    TradeCode = group['TradeCode'][group.index.max()]
    df = pd.DataFrame(farasahmDb['assetsCoustomerBroker'].find({'TradeCode':TradeCode,'dateInt':date}))
    nc = str(TradeCode)[4:]
    balanceRegister = farasahmDb['register'].find({'کد ملی':int(nc),'نماد کدال':symbol}).sort("تاریخ گزارش", DESCENDING).limit(1)
    balanceRegister = [x for x in balanceRegister]
    if len(balanceRegister)>0:
        group['balanceRegister'] = balanceRegister[0]['سهام کل']
    else:
        group['balanceRegister'] = 0

    name = farasahmDb['assetsCoustomerBroker'].find_one({'TradeCode':TradeCode},{'_id':0,'CustomerTitle':1})
    if len(df) == 0:
        group['balance'] = 0
    else:
        df['Symbol'] = df['Symbol'].apply(remove_non_alphanumeric)
        df = df[df['Symbol']==symbol]
    if len(df) == 0:
        group['balance'] = 0
    else:
        group['balance'] = int(df['Volume'][df.index.max()])
    
    if name !=None:
        group['CustomerTitle'] = name['CustomerTitle']
    elif len(balanceRegister)>0:
        group['CustomerTitle'] = balanceRegister[0]['نام خانوادگی ']
    else:
        group['CustomerTitle'] = 'نامشخص'

    return group

def convert_TradeCode_To_name(code):
    pass


def drop_duplicet_TradeListBroker(jalaliInt = todayIntJalali()):
    df = pd.DataFrame(farasahmDb['TradeListBroker'].find({"dateInt":jalaliInt},{'_id':0}))
    df = df.drop_duplicates(subset=['BranchID','MarketInstrumentISIN','NetPrice','Price','TotalCommission','TradeCode','TradeDate','TradeItemBroker','TradeNumber','TradeSymbol','TradeType','Volume'])
    if len(df)>0:
        farasahmDb['TradeListBroker'].delete_many({"dateInt":jalaliInt})
        farasahmDb['TradeListBroker'].insert_many(df.to_dict('records'))
        print('drop duplicets')


def comma_separate(input_str):
    if len(input_str) < 3:
        return input_str
    else:
        result_str = ','.join(input_str[-i-1:-i-4:-1] for i in range(0, len(input_str), 3))
        return result_str[::-1]
    


def  replace_values(row):
    now = str(JalaliDate.to_jalali(datetime.datetime.now()))
    if row['expier_reminderDate']:
        return now
    else:
        return row['jalali_reminderDate']
    


def Jdatetime_to_datetime(jdatetime):
    jdatetime = str(jdatetime).split('-')
    jdatetime = [int(x) for x in jdatetime]
    date = JalaliDate(jdatetime[0],jdatetime[1],jdatetime[2]).to_gregorian()
    return date