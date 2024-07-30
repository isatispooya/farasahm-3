from setting import farasahmDb
import pandas as pd
from bson import ObjectId
import json



def clean_list(data):
    cleaned_data = []
    for i in data:
        if isinstance(i, str) and i != 'nan' and not i.isdigit() and i != "":
            cleaned_data.append(i)
    return cleaned_data


#  registerNoBours لیست شهرهای فایل 
def city_nobourse(data):
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    

    city_list = farasahmDb['registerNoBours'].distinct('صادره')
    cleaned_city_list = clean_list(city_list)
    return cleaned_city_list




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



def fillter_registernobours (config) :
    print(config)
    if not config['enabled'] : 
        return pd.DataFrame()
    if len(config ['symbol']) >0 :
        df = farasahmDb['registerNoBours'].find({'symbol':{'$in':config['symbol']}})
    else :
        df = farasahmDb['registerNoBours'].find({})
        
    df = pd.DataFrame(df)
    print(df)
    return []




def fillter (data) :
    access = data['access'][0]
    symbol = data['access'][1]
    _id = ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    

    config = data['config']
    df_registernobours = fillter_registernobours(config['nobours'])

    return json.dumps([])