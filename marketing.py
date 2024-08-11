from setting import farasahmDb , pishkarDb
import pandas as pd
from bson import ObjectId
import json
from persiantools.jdatetime import JalaliDate
from datetime import datetime
from Login import SendSms
import requests
import math



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


def mobilePerfix(mobile_number, num1 ):
    if isinstance(mobile_number, str):
        if len(mobile_number) >= 7:
            for i in num1 :
                if str(i) in mobile_number[:4]:
                    return True
    return False


def mobileMidle(mobile_number, num2 ):
    if isinstance(mobile_number, str):
        if len(mobile_number) >= 7:
            for i in num2 :
                if isinstance(i, str) and i in mobile_number[4:7]:
                    return True
    return False


def name (name,abbreviation) :
    if not abbreviation:
        return True

    for i in abbreviation :

        if i in name :
            return True
    return False

def comp(company , comp):
    for i in comp :
        if i == company :
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


# customerofbroker لیست بانک های مشتریان کارگزاری از فایل 
def bank_broker(data) :
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    bank = farasahmDb['customerofbroker'].distinct('BankName')
    return bank

# customerofbroker لیست شعب  مشتریان کارگزاری از فایل 
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



# غیر بورسی
def fillter_registernobours (config ) :
    if not config['enabled'] : 
        return pd.DataFrame()
    if len(config ['symbol']) >0 :
        df = farasahmDb['registerNoBours'].find({'symbol':{'$in':config['symbol']}})
    else :
        df = farasahmDb['registerNoBours'].find({})
    df = pd.DataFrame(df)
    df = df.groupby(by='symbol').apply(filter_date_symbol)

    if len (config ['city']) >0 :
        df = df [df['صادره'].isin(config['city'])]

    if len (config['national_id']) >0 :
        df = df[df['کد ملی'].apply(lambda x:national_id(x, config['national_id']))]   

    if 'شماره تماس' in df.columns:
        df['شماره تماس'] = df['شماره تماس'].astype(str).str.replace("98", "0")
        df['شماره تماس'] = df['شماره تماس'].fillna("0")
    try:
        if 'mobile' in config and 'num1' in config['mobile'] and 'num2' in config['mobile']:
            if len(config['mobile']['num1'])>0:
                df = df[df['شماره تماس'].apply(lambda x: mobilePerfix(x, config['mobile']['num1']))]

            if len(config['mobile']['num2'])>0:
                df = df[df['شماره تماس'].apply(lambda x: mobileMidle(x, config['mobile']['num2']))]

    except:
        pass

    if len(df) ==0 :
        return df
    
    if config ['name'] :
        df['نام و نام خانوادگی'] = df['نام و نام خانوادگی'].fillna(0)
        df = df[df['نام و نام خانوادگی'].apply(lambda x :name(x,config['name']))]

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

    try:

        symbol_list = farasahmDb ['companyList'].find({'type' :'NoBourse'},{'_id':0 ,'symbol' : 1 , 'fullname' :1})
        symbol_list = [x for x in symbol_list]
        for i in symbol_list :
            df['symbol'] = df['symbol'].replace(i['symbol'], i['fullname'])
    except:
        pass

    df = df.reset_index(drop=True)
    df['درصد'] = df.groupby('symbol')['تعداد سهام'].transform(lambda x: x / x.sum() * 100)   
    df['درصد']=  df['درصد'].apply(int) 
    df['درصد']=  df['درصد'].apply(str) 
    if config['rate']['max']:
        max_rate = float(config['rate']['max'])
        df['درصد'] = df['درصد'].fillna(0)
        df['درصد'] = df['درصد'].astype(float)
        max = float(config['rate']['max'])
        df = df[df['درصد'] >= max]
    if config['rate']['min']:
        df['درصد'] = df['درصد'].fillna(0)
        df['درصد'] = df['درصد'].astype(float)
        min = float(config['rate']['min'])
        df = df[df['درصد'] >= min]
    if 'rate' in df.columns:
        df = df.drop(columns='rate')
    if '_id' in df.columns:
        df = df.drop(columns='_id')
    df = df.rename(columns = {'symbol' :'نام شرکت', 'rate' :'درصد'})
    df = df.fillna('')
    return df



# get مورد بیمه
def insurance_item(data):
    access = data['access'][0]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    insuring_item = pishkarDb['Fees'].distinct('مورد بیمه')
    insuring_item = [i for i in insuring_item if i is not None and not (isinstance(i, float) and math.isnan(i))]
    return   insuring_item



# get رشته بیمه 
def Insurance_field(data):
    access = data['access'][0]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    Insurance_field = pishkarDb['Fees'].distinct('رشته')
    Insurance_field = list(set(item.strip() for item in Insurance_field))
    return   Insurance_field


# get شرکت های بیمه گر 
def insurance_companies (data) :
    access = data['access'][0]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    insurance_companies = pishkarDb['AssingIssuing'].distinct('comp')
    return insurance_companies


# get  مشاوران بیمه 
def insurance_consultant (data) :
    access = data['access'][0]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    consultant = pishkarDb['sub'].distinct('name')
    return consultant
 

# بیمه 
def fillter_insurance(config):
    if not config['enabled']:
        return pd.DataFrame()

    df = pishkarDb['customers'].find({}, {'_id': 0, 'بيمه گذار': 1, 'تلفن همراه': 1, 'کد ملي بيمه گذار': 1, 'comp': 1})
    df = pd.DataFrame(df)
    df = df.rename(columns={'بيمه گذار': 'نام و نام خانوادگی', 'کد ملي بيمه گذار': 'کد ملی', 'تلفن همراه': 'شماره تماس', 'comp': 'بیمه گر'})
    
    df = df[df['نام و نام خانوادگی'].apply(lambda x: name(x, config['name']))]
    
    if len(config['company']) > 0:
        df = df.loc[df['بیمه گر'].apply(lambda x: comp(x, config['company']))]

    if 'شماره تماس' in df.columns:
        df['شماره تماس'] = df['شماره تماس'].astype(str).str.replace("98", "0")
        df['شماره تماس'] = df['شماره تماس'].fillna("0")

    try:
        if 'mobile' in config and 'num1' in config['mobile'] and 'num2' in config['mobile']:
            if len(config['mobile']['num1']) > 0:
                df = df[df['شماره تماس'].apply(lambda x: mobilePerfix(x, config['mobile']['num1']))]
            if len(config['mobile']['num2']) > 0:
                df = df[df['شماره تماس'].apply(lambda x: mobileMidle(x, config['mobile']['num2']))]
    except Exception as e:
        print(f"Error processing mobile filtering: {e}")
    
    if 'national_id' in config and len(config['national_id']) > 0:
        if 'کد ملی' in df.columns:
            df = df[df['کد ملی'].apply(lambda x: national_id(x, config['national_id']))]
        else:
            print("Warning: 'کد ملی' column is missing in the DataFrame")
    
    if df.empty:
        print("No matching customers found.")
        return pd.DataFrame()
    
    df = df.dropna(subset=['نام و نام خانوادگی'])

    if len(config['insurance_item']) > 0:
        df_fees = pishkarDb['Fees'].find({'مورد بیمه': {'$in': config['insurance_item']}})
    else:
        df_fees = pishkarDb['Fees'].find({})

    df_fees = pd.DataFrame(df_fees)
    df_fees = df_fees.rename(columns={'مورد بیمه': 'insurance_item'})

    if 'insurance_item' not in df_fees.columns:
        return "Error: 'insurance_item' column is missing in df_fees."

    df = pd.merge(df, df_fees, how='inner', left_on='نام و نام خانوادگی', right_on='بيمه گذار')

    if 'insurance_item' not in df.columns:
        return "Error: 'insurance_item' column is missing after merging."

    if len(config['insurance_field']) > 0:
        df = df[df['رشته'].apply(lambda x: x in config['insurance_field'])]
        if df.empty:
            print("No matching insurance fields found.")
            return pd.DataFrame()
    
    df = df[["نام و نام خانوادگی", "کد ملی_x", "بیمه گر", "شماره تماس", "رشته", "insurance_item", "شماره بيمه نامه"]]
    df = df.rename(columns={'insurance_item': 'مورد بیمه', 'کد ملی_x': 'کد ملی'})


    df_assing = pishkarDb['assing'].find({}, {'consultant': 1, 'شماره بيمه نامه': 1, '_id': 0})
    df_assing = pd.DataFrame(df_assing)

    if len(config['consultant']) > 0:
        df_cons = pd.DataFrame(list(pishkarDb['cunsoltant'].find({}, {'_id': 0, 'nationalCode': 1, 'fristName': 1, 'lastName': 1})))
    else:
        df_cons = pd.DataFrame(list(pishkarDb['cunsoltant'].find({})))
    df_cons['مشاور'] = df_cons['fristName'] + ' ' + df_cons['lastName']
    df_cons = pd.merge(df_assing, df_cons, how='inner', left_on='consultant', right_on='nationalCode')
    df_cons = df_cons[['مشاور', 'شماره بيمه نامه']]
    df = pd.merge(df_cons, df, how='inner', on='شماره بيمه نامه')
    df  = df[df['مشاور'].apply(lambda x : x in config['consultant'])]
    if df.empty :
        return pd.DataFrame()
    df = df.drop(columns='شماره بيمه نامه' , axis=1)
    if len(df) > 100000 :
        return json.dumps({'message': 'تنظیمات با موفقیت ذخیره شد. به علت حجم بالای داده، قابل نمایش نیست.'})

    print(df)
    return df



def set_status (data) :
    access = data['access'][0]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    
    status = farasahmDb['marketing_config'].find_one({'user': access, '_id': ObjectId(data['_id'])}, {'_id': 0, 'status': 1})

    
    if status is None:
        return json.dumps({"reply": False, "msg": 'تنظیمات یافت نشد'})

    update_result = farasahmDb['marketing_config'].update_one(
        {"user": access, "_id": ObjectId(data['_id'])},
        {"$set": {"status": status['status']}}
    )

    if update_result.matched_count == 0:
        return json.dumps({"reply": False, "msg": 'خطا در به‌روزرسانی تنظیمات'})

    return json.dumps({'reply': True, 'msg': 'وضعیت با موفقیت به‌روزرسانی شد'})








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
    insurance = fillter_insurance(config['insurance'])
    df = pd.concat([registernobours,insurance])
    for i in df.columns : 
        if i in ['index', '_id' , 'date'] :
            df = df.drop(columns=i)
    df_column = ["{{" + str(x) + "}}" for x in df.columns]
    df = pd.DataFrame(df)
    len_df = len(df)
    dict_df = df.to_dict('records')

    return json.dumps({'reply' : True , 'columns' : df_column , 'dic' : dict_df , 'len' :len_df })



def replace_placeholders(row , context):
    return context.replace("{{", "{").replace("}}", "}").format_map(row)




def ViewConfig(data):
    access = data['access'][0]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _id = ObjectId(data['_id'])
    column  = farasahmDb ['marketing_config'].find_one({'user' :access , '_id' : ObjectId(data['_id'])} , {'_id':0 , 'config' : 1 , 'title' :1})
    if column is None:
        return json.dumps({'reply': False, 'msg': 'یافت نشد'})
    config  = column['config']
    title = column['title']
    return json.dumps({'config' :config  ,'title' : title,})




def perViewContent(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _id = ObjectId(data['_id'])
    column  = farasahmDb ['marketing_config'].find_one({'user' :access , '_id' : ObjectId(data['_id'])} , {'_id':0 , 'config' : 1 , 'title' :1, 'context' :1})
    if column is None:
        return json.dumps({'reply': False, 'msg': 'یافت نشد'})
    config  = column['config']
    title = column['title']
    df_registernobours = fillter_registernobours(config['nobours'])
    df_insurance = fillter_insurance(config['insurance'])

    df = pd.concat([df_registernobours,df_insurance])
    len_df  = len(df)

    # registernobours = fillter_registernobours(config['nobours'])

    df = df.fillna('')
    df['result'] = df.apply(replace_placeholders, args=(column['context'],), axis=1)
    df['count_sms'] = [len(x)/70 for x in df['result']]
    df['count_sms'] = df['count_sms'].apply(math.ceil)
    count_sms = df['count_sms'].sum()
    count_sms = int(count_sms)
    if count_sms>0:
        cost = [int(count_sms*78),'الی', int(count_sms*150)]
    else:
        cost = [0, 0]

    df = df.drop(columns=['count_sms'])
    context = column['context']
    # df = df.head(n=2)
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
    if 'index' in df.columns:
        df = df.drop(columns=['index'])
    df = df.fillna('')
    column = df.columns
    column = list(column)
    df = df.to_dict('records')
    return json.dumps({'dict': df , 'config' :config  ,'title' : title, 'len' : len_df, 'count_sms':count_sms, 'cost':cost ,"column" :column , 'context' :context })


def send_message(data):
    access = data['access'][0]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    _id = ObjectId(data['_id'])
    column  = farasahmDb ['marketing_config'].find_one({'user' :access , '_id' : ObjectId(data['_id'])} , {'_id':0 , 'config' : 1})
    if column is None:
        return json.dumps({'reply': False, 'msg': 'یافت نشد'})
    context = data['context']
    column  = farasahmDb ['marketing_config'].update_one({'user' :access , '_id' : ObjectId(data['_id'])} , {'$set':{'status':True, 'context':context}})
    return json.dumps({'reply' : True})





def marketing_list (data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})

    marketing_list = farasahmDb ['marketing_config'].find({'user':access} , {'_id':1 ,'title' : 1, 'status':1 , 'date' :1 ,'period' :1 , 'config':1})
    marketing_list = [{'_id' : str(x['_id']),'title' : x['title'] ,'period': x['config']['period'], 'status' : x['status'] ,'date': JalaliDate(x['date']).strftime('%Y/%m/%d') , 'time': x['date'].strftime('%H:%M:%S')} for x in marketing_list]
    return json.dumps (marketing_list)


def fillter (data) :

    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    if not data['config']:
        return json.dumps({"reply" : False , "msg" : 'تنظیمات ارسال نشده'})
    if not data['title']:
        return json.dumps({"reply" : False , "msg" : 'عنوان ارسال نشده'})
    if len(data['title']) == 0:
        return json.dumps({"reply" : False , "msg" : 'عنوان ارسال نشده'})
    if not data['config']['send_time']:
        return json.dumps({"reply" : False , "msg" : 'زمان اولین ارسال تنظیم نشده'})
    if not data['config']['period']:
        return json.dumps({"reply" : False , "msg" : 'دوره ارسال تنظیم نشده'})
    if not data['config']['period'] in ['once','daily','weekly','monthly']:
        return json.dumps({"reply" : False , "msg" : 'دوره ارسال به درستی تنظیم نشده'})
    
    config = data['config']
    title = data['title']
    avalibale = farasahmDb['marketing_config'].find_one({"user" :access,"title":title})
    if avalibale:
        return json.dumps({"reply" : False , "msg" : 'تنظیمات با این عنوان موجود است'})
    avalibale = farasahmDb['marketing_config'].find_one({"user" :access,"config":config})
    if avalibale:
        return json.dumps({"reply" : False , "msg" : 'تنظیمات تکراری است'} )
    df_registernobours = fillter_registernobours(config['nobours'])
    df_insurance = fillter_insurance(config['insurance'])
    df = pd.concat([df_registernobours,df_insurance])
    df = df.fillna('')
    len_df  = len(df)
    df = df.to_dict('records')
    
    date = datetime.now()
    farasahmDb['marketing_config'].insert_one({"user" :access , "config" :config,"title":title,'date':date, 'status':False, 'context':''})
    id = farasahmDb['marketing_config'].find_one({"user" :access , "config" :config,"title":title,'date':date, 'status':False, 'context':'' },{'_id':1})['_id']
    id = str(id)
    return json.dumps({"reply" : True , "df" : df , "len" : len_df ,'id' :id} )


def edit_context (data) :

    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id': _id}, {'_id': 0})
    if acc is None:
        return json.dumps({'reply': False, 'msg': 'کاربر یافت نشد لطفا مجددا وارد شوید'})

    context = data.get('context')
    id = ObjectId(data['_id'])

    if  not id or context is None :
        return json.dumps({'reply': False, 'msg': 'داده‌های ورودی ناقص است'})
    
    update_result = farasahmDb['marketing_config'].update_one(
        {"user": access, "_id": id},
        {"$set": {"context": context}}
    )
    
    if update_result.matched_count == 0:
        return json.dumps({"reply": False, "msg": 'خطا در به‌روزرسانی تنظیمات'})
    return json.dumps ({'reply' :True , 'context' : context})
    




def edit_config(data):
    try:
        access = data['access'][0]
        symbol = data['access'][1]
        _id = ObjectId(access)
        acc = farasahmDb['user'].find_one({'_id': _id}, {'_id': 0})
        if acc is None:
            return json.dumps({'reply': False, 'msg': 'کاربر یافت نشد لطفا مجددا وارد شوید'})

        config = data.get('config')
        id = ObjectId(data['_id'])


        send_time = config.get('send_time') if config else None
        period = config.get('period') if config else None
        
        if not config or not id or send_time is None or period is None:
            return json.dumps({'reply': False, 'msg': 'داده‌های ورودی ناقص است'})

        existing_config = farasahmDb['marketing_config'].find_one({"user": access, "_id": id})
        if not existing_config:
            return json.dumps({"reply": False, "msg": 'تنظیماتی با این عنوان یافت نشد'})

        update_result = farasahmDb['marketing_config'].update_one(
            {"user": access, "_id": id},
            {"$set": {"config": config, "send_time": send_time, "period": period, "date": datetime.now()}}
        )
        
        if update_result.matched_count == 0:
            return json.dumps({"reply": False, "msg": 'خطا در به‌روزرسانی تنظیمات'})

        # df_registernobours = fillter_registernobours(config['nobours'])
        # df_insurance = fillter_insurance(config['insurance'])
        # df = pd.concat([df_registernobours,df_insurance])
        # df = df.fillna('')
        # len_df = len(df)
        # df = df.to_dict('records')
        
        return json.dumps({"reply": True, "df": "df", "len": "len_df"})
    
    except KeyError as e:
        return json.dumps({'reply': False, 'msg': f"کلید {str(e)} در داده‌های ورودی یافت نشد"})
    except Exception as e:
        return json.dumps({'reply': False, 'msg': f"خطای غیرمنتظره: {str(e)}"})


def delete_config(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id': _id}, {'_id': 0})
    if acc is None:
        return json.dumps({'reply': False, 'msg': 'کاربر یافت نشد لطفا مجددا وارد شوید'})

    id = ObjectId(data['_id'])

    existing_config = farasahmDb['marketing_config'].find_one({"user": access, "_id": id})
    if not existing_config:
        return json.dumps({"reply": False, "msg": 'تنظیماتی با این عنوان یافت نشد'})

    delete_result = farasahmDb['marketing_config'].delete_one({"user": access, "_id": id})

    if delete_result.deleted_count == 1:
        return json.dumps({"reply": True, "msg": "تنظیمات با موفقیت حذف شد"})
    else:
        return json.dumps({"reply": False, "msg": "تنظیمات یافت نشد یا قبلاً حذف شده است"})