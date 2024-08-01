from setting import farasahmDb
import pandas as pd
from bson import ObjectId
import json
from persiantools.jdatetime import JalaliDate
from datetime import datetime
from Login import SendSms

def clean_list(data):
    cleaned_data = []
    for i in data:
        if isinstance(i, str) and i != 'nan' and not i.isdigit() and i != "":
            cleaned_data.append(i)
    return cleaned_data

def national_id (national_id , code) :
    if isinstance(national_id, str):
        for i in code:
            if i in national_id:
                return True
    return False



def mobile(mobile_number, num1, num2 ):
    if isinstance(mobile_number, str):
        if len(mobile_number) >= 7:
            for i in num1 :
                if isinstance(i, str) and i in mobile_number[1:4]:
                    return True
            for i in num2 :
                if isinstance(i, str) and i in mobile_number[4:7]:
                    return True

    return False




def filter_date_symbol(df):
    df['date'] = df['date'].fillna(0)
    df['date'] = df['date'].apply(int)
    df = df[df['date']==df['date'].max()]
    df = df.reset_index()
    return df


#  customerofbroker , registerNoBours لیست شهرهای فایل 
def city_nobourse(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    

    city_list = farasahmDb['registerNoBours'].distinct('صادره')
    city_list_broker = farasahmDb['customerofbroker'].distinct('AddressCity')
    city_birthday_list_broker = farasahmDb['customerofbroker'].distinct('BirthCertificateCity')
    cleaned_city_list = city_list + city_list_broker + city_birthday_list_broker
    cleaned_city_list = clean_list(cleaned_city_list)


    return cleaned_city_list



def bank_broker(data) :
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    bank = farasahmDb['customerofbroker'].distinct('BankName')
    return bank


def broker_branch(data) :
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    branch = farasahmDb['customerofbroker'].distinct('BrokerBranch')
    return branch


#  companyList لیست شرکت های  فایل 
def symbol_nobours (data) :
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})


    symbol_list = farasahmDb ['companyList'].find({'type' :'NoBourse'},{'_id':0 ,'symbol' : 1 , 'fullname' :1})
    symbol_list = [x for x in symbol_list]
    return json.dumps(symbol_list)




def fillter_registernobours (config ) :
    if not config['enabled'] : 
        return pd.DataFrame()
    if len(config ['symbol']) >0 :
        df = farasahmDb['registerNoBours'].find({'symbol':{'$in':config['symbol']}})
    else :
        df = farasahmDb['registerNoBours'].find({})
    df = pd.DataFrame(df)
    df = df.groupby(by='symbol').apply(filter_date_symbol)

    df = df.reset_index(drop=True)
    df['درصد'] = df.groupby('symbol')['تعداد سهام'].transform(lambda x: x / x.sum() * 100)   

    if config['rate']['max']:
        df['درصد'] = df['درصد'].fillna(0)
        df['درصد'] = df['درصد'].astype(float)
        max = float(config['rate']['max'])
        df = df[df['درصد'] >= max]
    if config['rate']['min']:
        df['درصد'] = df['درصد'].fillna(0)
        df['درصد'] = df['درصد'].astype(float)
        min = float(config['rate']['min'])
        df = df[df['درصد'] <= min]


    if len (config ['city']) >0 :
        df = df [df['صادره'].isin(config['city'])]

    if len (config['national_id']) >0 :
        df = df[df['کد ملی'].apply(lambda x:national_id(x, config['national_id']))]       


    if 'mobile' in config and 'num1' in config['mobile'] and 'num2' in config['mobile']:
        df['شماره تماس'] = df['شماره تماس'].astype(str).str.replace("98", "0")
        df['شماره تماس'] = df['شماره تماس'].fillna("0")
        df = df[df['شماره تماس'].apply(lambda x: mobile(x, config['mobile']['num1'], config['mobile']['num2']))]



    if config['birthday']['from'] :
        from_date = JalaliDate.fromtimestamp(int(config['birthday']['from']/1000))
        from_date = int(str(from_date).replace('-',''))
        df['تاریخ تولد'] = df['تاریخ تولد'].fillna(0)
        df['تاریخ تولد'] = [str(x).replace('/','') for x in df['تاریخ تولد']]
        df['تاریخ تولد'] = df['تاریخ تولد'].apply(int)
        df = df[df['تاریخ تولد']>=from_date]
    if config['birthday']['to'] :
        to_date = JalaliDate.fromtimestamp(int(config['birthday']['to']/1000))
        to_date = int(str(to_date).replace('-',''))
        df['تاریخ تولد'] = df['تاریخ تولد'].fillna(0)
        df['تاریخ تولد'] = [str(x).replace('/','') for x in df['تاریخ تولد']]
        df['تاریخ تولد'] = df['تاریخ تولد'].apply(int)
        df = df[df['تاریخ تولد']<=to_date]


    if config['amount']['from'] :
        df['تعداد سهام'] = df['تعداد سهام'].fillna(0)
        df['تعداد سهام'] = df['تعداد سهام'].astype(int)
        from_amount = int(config['amount']['from'])
        df = df[df['تعداد سهام'] >= from_amount]
    if config['amount']['to'] :
        df['تعداد سهام'] = df['تعداد سهام'].fillna(0)
        df['تعداد سهام'] = df['تعداد سهام'].astype(int)    
        to_amount = int(config['amount']['to'])
        df = df[df['تعداد سهام'] <= to_amount]


        
    symbol_list = farasahmDb ['companyList'].find({'type' :'NoBourse'},{'_id':0 ,'symbol' : 1 , 'fullname' :1})
    symbol_list = [x for x in symbol_list]
    for i in symbol_list :
        df['symbol'] = df['symbol'].replace(i['symbol'], i['fullname'])
    df = df.rename(columns = {'symbol' :'نام شرکت', 'rate' :'درصد'})
    df = df.fillna('')

    return df




def column_marketing (data) :
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    column  = farasahmDb ['marketing_config'].find_one({'user' :access , '_id' : ObjectId(data['_id'])} , {'_id':0 , 'config' : 1})
    if column is None:
        return json.dumps({'reply': False, 'msg': 'یافت نشد'})
    config  = column['config']
    registernobours = fillter_registernobours(config['nobours'])

    for i in registernobours.columns : 
        if i in ['index', '_id' , 'date'] :
            registernobours = registernobours.drop(columns=i)


    registernobours_column = ["{{" + str(x) + "}}" for x in registernobours.columns]

    df = pd.DataFrame(registernobours)
    len_df = len(df)
    dict_df = df.to_dict('records')

    return json.dumps({'reply' : True , 'columns' : registernobours_column , 'dic' : dict_df , 'len' :len_df })



def replace_placeholders(row , context):
    return context.replace("{{", "{").replace("}}", "}").format_map(row)




def perViewContent(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _id = ObjectId(data['_id'])
    column  = farasahmDb ['marketing_config'].find_one({'user' :access , '_id' : ObjectId(data['_id'])} , {'_id':0 , 'config' : 1})
    if column is None:
        return json.dumps({'reply': False, 'msg': 'یافت نشد'})
    config  = column['config']
    registernobours = fillter_registernobours(config['nobours'])
    registernobours = registernobours.head(n=2)
    context = data['context']
    registernobours['result'] = registernobours.apply(replace_placeholders, args=(context,), axis=1)
    if '_id' in registernobours.columns:
        registernobours['_id'] = registernobours['_id'].astype(str)

    dict_registernobours = registernobours.to_dict('records')
    return json.dumps({'dict': dict_registernobours})


def send_message(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    config  = data['config']
    context = data['context']
    phone_number = "  "
    message_text = context

    response = SendSms(phone_number, message_text)

    if response.get('status') == 'OK':
        return json.dumps({'reply': True, 'msg': 'پیام با موفقیت ارسال شد'})
    else:
        return json.dumps({'reply': False, 'msg': 'مشکلی در ارسال پیام به وجود آمد'})





def marketing_list (data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})

    marketing_list = farasahmDb ['marketing_config'].find({'user':access} , {'_id':1 ,'title' : 1})
    marketing_list = [{'_id' : str(x['_id']),'title' : x['title'] } for x in marketing_list]
    return json.dumps (marketing_list)


def fillter (data) :
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    

    config = data['config']
    title = data['title']
    avalibale = farasahmDb['marketing_config'].find_one({"user" :access,"title":title})
    if avalibale:
        return json.dumps({"reply" : False , "msg" : 'تنظیمات با این عنوان موجود است'})
    avalibale = farasahmDb['marketing_config'].find_one({"user" :access,"config":config})
    if avalibale:
        return json.dumps({"reply" : False , "msg" : 'تنظیمات تکراری است'} )
    df_registernobours = fillter_registernobours(config['nobours'])
    len_registernobours  = len(df_registernobours)
    df_registernobours=df_registernobours.to_dict('records')
    date = datetime.now()
    farasahmDb['marketing_config'].insert_one({"user" :access , "config" :config,"title":title,'date':date})


    return json.dumps({"reply" : True , "df" : df_registernobours , "len" : len_registernobours} )


