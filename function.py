from persiantools.jdatetime import JalaliDate
import datetime
import pandas as pd
import requests
from persiantools.jdatetime import JalaliDate
from persiantools import characters, digits
import pymongo
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

def gorgianIntToJalaliInt(date):
    y = str(date)[:4]
    m = str(date)[4:6]
    d = str(date)[6:8]
    Jalali = (JalaliDate.to_jalali(int(y),int(m),int(d)))
    return int(str(Jalali).replace('-',''))

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
    

def toDayJalaliListYMD():
    Today = datetime.datetime.now()
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
    
def getTseToday():
    today = datetime.datetime.now()
    dt = datetime.datetime(today.year,today.month,today.day,15,0,0)
    jalali = JalaliDate.to_jalali(today)
    jalaliStr = str(jalali).replace('-','/')
    jalaliInt =int(str(jalali).replace('-',''))
    #res = requests.get(url=f'http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d={jalali}')
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




    

def Apply_Trade_Symbol(group,symbol):
    group['NetPrice_Buy'] = group[group['TradeType']=='Buy']['NetPrice'].sum()
    group['Volume_Buy'] = group[group['TradeType']=='Buy']['Volume'].sum()
    group['Price_Buy'] = group['NetPrice_Buy'] / group['Volume_Buy']
    group['TotalCommission_Buy'] = group[group['TradeType']=='Buy']['TotalCommission'].sum()
    group['NetPrice_Sell'] = group[group['TradeType']=='Sell']['NetPrice'].sum()
    group['Volume_Sell'] = group[group['TradeType']=='Sell']['Volume'].sum()
    group['Price_Sell'] = group['NetPrice_Sell'] / group['Volume_Sell']
    group['TotalCommission_Sell'] = group[group['TradeType']=='Sell']['TotalCommission'].sum()
    group = group.drop(columns=['NetPrice','Price','TotalCommission','TradeDate','TradeStationType','TradeType','TransferTax','Volume'])
    group = group[group.index==group.index.max()]
    group = group.fillna(0)
    print(group)
    #df = farasahmDb['assetsCoustomerBroker'].find()
    return group

def convert_TradeCode_To_name(code):
    pass


def drop_duplicet_TradeListBroker():
    today = datetime.datetime.now()
    jalali = JalaliDate.to_jalali(today)
    jalaliInt =int(str(jalali).replace('-',''))
    df = pd.DataFrame(farasahmDb['TradeListBroker'].find({"dateInt":jalaliInt},{'_id':0}))
    df = df.drop_duplicates(subset=['BranchID','MarketInstrumentISIN','NetPrice','Price','TotalCommission','TradeCode','TradeDate','TradeItemBroker','TradeNumber','TradeSymbol','TradeType','Volume'])
    farasahmDb['TradeListBroker'].delete_many({"dateInt":jalaliInt})
    farasahmDb['TradeListBroker'].insert_many(df.to_dict('records'))