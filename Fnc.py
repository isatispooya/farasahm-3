from persiantools.jdatetime import JalaliDate
import datetime
import pandas as pd
import requests
from persiantools.jdatetime import JalaliDate
from persiantools import characters, digits
import pymongo
from pymongo import ASCENDING,DESCENDING
import time
import threading
import Fnc

client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

def retry_decorator(max_retries=3, sleep_duration=10):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(max_retries + 1):
                try:
                    thread = threading.Thread(target=func, args=args, kwargs=kwargs)
                    thread.start()
                    thread.join()
                    break  # اگر تابع بدون مشکل اجرا شود، از حلقه خارج شود
                except Exception as e:
                    print(f'Error: {e}')
                    time.sleep(sleep_duration)  # تاخیر دهید و دوباره تلاش کنید

        return wrapper
    return decorator


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


def jalaliStrDifToday(date):
    try:
        lst = str(date).split('-')
        if len(lst) == 3:
            lst = [int(x) for x in lst]
            date = JalaliDate(lst[0],lst[1],lst[2]).to_gregorian()
            date = str(date).split('-')
            date = [int(x) for x in date]
            date = datetime.datetime(date[0],date[1],date[2])
            day = date - datetime.datetime.now()
            day = day.days
            return day
        return 0
    except:
        return 0


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

@retry_decorator(max_retries=3, sleep_duration=5)
def getTseDate(date=datetime.datetime.now()):
    dt = datetime.datetime(date.year,date.month,date.day,15,0,0)
    jalali = JalaliDate.to_jalali(date)
    jalaliStr = str(jalali).replace('-','/')
    jalaliInt =int(str(jalali).replace('-',''))
    avalibale = farasahmDb['tse'].find_one({'dataInt':jalaliInt})
    print('start get tse in', jalaliInt,'for sandoq')
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
                
@Fnc.retry_decorator(max_retries=3, sleep_duration=5)
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


def has_number(input_str):
    has_digit = any(char.isdigit() for char in input_str)
    return has_digit

def is_Fund(name):
    fund_type_a = 'صندوق س' in name
    if fund_type_a:
        return True
    fund_type_b = 'ص.س.' in name
    if fund_type_b:
        return True
    return False

def type_fund(name):
    if 'بخشي' in name:
        return 'saham'
    elif 'املاك' in name:
        return 'amlak'
    elif 'مستغلات' in name:
        return 'amlak'
    elif 'زمين و ساختمان' in name:
        return 'amlak'
    elif 'درسهام' in name:
        return 'saham'
    elif 'درآمد ثابت' in name:
        return 'sabet'
    elif name[-2:] == '-د':
        return 'sabet'
    elif name[-2:] == '-م':
        return 'saham'
    elif '-ثابت' in name:
        return 'sabet'
    elif name[-2:] == '-س':
        return 'saham'
    elif 'صندوق در صندوق' in name:
        return 'sandoq'
    elif 'طلاي سرخ' in name:
        return 'kala'
    elif 'طلا' in name:
        return 'kala'
    elif 'زعفران' in name:
        return 'kala'
    elif 'جسورانه' in name:
        return 'jasor'
    elif 'پارند پايدار سپهر' in name:
        return 'sabet'
    else:
        return 'not fund'
    

    


def fund_compare_clu_ccp(group):
    group = group.sort_values(by=['dateInt'],ascending=False)
    group['changeClosePrice'] = group['close_price'] - group['close_price'].shift(-1)
    group['changeClosePrice'] = group['changeClosePrice'].fillna(0)
    group['changeClosePriceRatre'] = group['changeClosePrice'] / group['close_price']
    group['changeClosePriceRatre'] = group['changeClosePriceRatre'].fillna(0)
    tagsim_sod = group['changeClosePrice'].min()<0
    if tagsim_sod:
        return pd.DataFrame()
        group = group.sort_values(by=['dateInt'])
        group['tagsim_sod'] = group['changeClosePrice'].apply(lambda x: x if x < 0 else 0)
        group['tagsim_sod'] = group['tagsim_sod'][::-1].cumsum()[::-1]
        group['tagsim_sod'] = group['tagsim_sod'] * -1
        group['close_price'] = group['tagsim_sod'] + group['close_price']
        group = group.drop(columns=['tagsim_sod'])
    periodList = [7,14,30,90,180,365,730]
    dic = {}
    for i in periodList:
        endDateJalali = group['dateInt'].max()
        endDate = dateIntJalaliToGorgian(endDateJalali)
        endDate = datetime.datetime.strptime(endDate, '%Y-%m-%dT%H:%M:%S')
        startDate = endDate - datetime.timedelta(days=i)
        startDateJalali = gorgianIntToJalaliInt(startDate)
        startDateJalali = group[group['dateInt']>=startDateJalali]
        startDateJalali = startDateJalali['dateInt'].min()
        diff_date = dateIntJalaliToGorgian(startDateJalali)
        diff_date = (datetime.datetime.strptime(diff_date, '%Y-%m-%dT%H:%M:%S')  - startDate).days
        if abs(diff_date)>1:
            dic[f'ret_period_{i}'] = 0
            dic[f'ret_ytm_{i}'] = 0
            dic[f'ret_smp_{i}'] = 0
        else:
            end_price = group[group['dateInt'] == endDateJalali]['close_price'].values[0]
            start_price = group[group['dateInt'] == startDateJalali]['close_price'].values[0]
            rate_return_in_period = end_price / start_price
            print(endDateJalali,startDateJalali,endDateJalali>startDateJalali,i,'-',end_price,start_price,end_price>start_price)
            rate_return_yearly_ytm = (rate_return_in_period ** (365/i)-1)
            rate_return_yearly_smp = (rate_return_in_period - 1) * (365/i)
            dic[f'ret_period_{i}'] = int((rate_return_in_period-1) * 10000) / 100
            dic[f'ret_ytm_{i}'] = int(rate_return_yearly_ytm * 10000) / 100
            dic[f'ret_smp_{i}'] = int(rate_return_yearly_smp * 10000) / 100



    
    group = pd.DataFrame([dic])

    return group




def setTypeInFundBySymbol(symbol):
    absSymbol = str(symbol)[:-1]
    hasNumber = has_number(absSymbol)
    if not hasNumber:
        return 'saham'
    for i in ['اخزا', 'اراد', 'افاد']:
        if i in symbol:
            return 'gov'
    return 'non-gov'




def calnder():
    end_date = datetime.datetime(2026,1,1)
    cruses = datetime.datetime(2023,10,23)
    holiday = ['1402-09-26','1402-11-22','1402-12-06','1403-01-01','1403-01-04','1403-01-12','1403-01-13',
           '1403-02-16','1403-02-14','1403-02-15','1403-02-28','1403-04-05','1403-04-26','1403-04-27',
           '1403-06-04','1403-06-12','1403-06-14','1403-06-31','1403-10-25','1403-11-09','1403-11-22',
           '1403-11-27','1403-12-29','1404-01-02','1404-01-03','1404-01-04','1404-01-11','1404-01-12',
           '1404-01-13','1404-03-14','1404-03-17','1404-03-25','1404-04-14','1404-04-15','1404-06-01',
           '1404-06-02','1404-06-10','1404-06-19','1404-09-04','1404-10-13','1404-10-27',
           ]
    clnd = []
    while cruses <= end_date:
        ja_date = JalaliDate(cruses.date())
        week = ja_date.weekday()
        cruses = cruses + datetime.timedelta(days=1)
        workday = True
        if int(week)>=5:
            workday = False
        if str(ja_date) in holiday:
            workday = False
        dic = {'date':str(cruses.date()),'ja_date':str(ja_date),'week':week,'workday':workday}
        clnd.append(dic)
    return clnd

def calculate_future_holidays(row, df):
    if row['workday']:
        future_holidays_count = 0
        idx = row.name + 1  # شروع از ردیف بعدی
        while idx < len(df) and not df.loc[idx, 'workday']:
            future_holidays_count += 1
            idx += 1
        return future_holidays_count
    else:
        return 0
    

def calculate_past_holidays(row, df):
    if row['workday']:
        past_holidays_count = 0
        idx = row.name - 1  # شروع از ردیف قبلی
        while idx >= 0 and not df.loc[idx, 'workday']:
            past_holidays_count += 1
            idx -= 1
        return past_holidays_count
    else:
        return 0
    
def fiscal_id_to_number(fiscal_id):
    result = ''
    for char in fiscal_id:
        if str.isdigit(char):
            result += char
        else:
            result += str(ord(char))
    return result


MULTIPLICATION_TABLE = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
    [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
    [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
    [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
    [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
    [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
    [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
    [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
    [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
]

PERMUTATION_TABLE = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 5, 7, 6, 2, 8, 3, 0, 9, 4],
    [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
    [8, 9, 1, 6, 0, 4, 3, 5, 2, 7],
    [9, 4, 5, 3, 1, 2, 6, 8, 7, 0],
    [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
    [2, 7, 9, 3, 8, 0, 6, 4, 1, 5],
    [7, 0, 4, 6, 9, 1, 3, 2, 5, 8],
]

INVERSE_TABLE = [0, 4, 3, 2, 1, 5, 6, 7, 8, 9]

def checkSum(number):
    c = 0
    len_ = len(number)
    for i in range(len_):
        c = MULTIPLICATION_TABLE[c][PERMUTATION_TABLE[(
            (i + 1) % 8)][int(number[len_ - i - 1]) - 0]]
    return INVERSE_TABLE[c]


def generate_tax_id(fiscal_id, date, internal_invoice_id):
    days_past_epoch = (date.date() - datetime.datetime(1970, 1, 1).date()).days
    days_past_epoch_padded = str(days_past_epoch).rjust(6, "0")
    hex_days_past_epoch_padded = str(f'{days_past_epoch:x}').rjust(5, "0")
    numeric_fiscal_id = fiscal_id_to_number(fiscal_id)
    internal_invoice_id_padded = str(internal_invoice_id).rjust(12, '0')
    hex_internal_invoice_id_padded = str(
        f'{int(internal_invoice_id):x}').rjust(10, '0')
    decimal_invoice_id = str(numeric_fiscal_id) + \
        str(days_past_epoch_padded) + str(internal_invoice_id_padded)
    checksum = checkSum(decimal_invoice_id)
    return (fiscal_id + str(hex_days_past_epoch_padded) + str(hex_internal_invoice_id_padded) + str(checksum)).upper()



def generatIdInternal(idstr):
    lst = farasahmDb['idintrnalMoadian'].find({"id":idstr})
    lst = [x for x in lst]
    if len(lst) == 0:
        result = 1
    else:
        lst = [x["idintrnal"] for x in lst]
        result = max(lst) + 1
    farasahmDb['idintrnalMoadian'].insert_one({'id':idstr,'idintrnal':result})
    return result