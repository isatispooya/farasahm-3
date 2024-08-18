
import json
import zipfile
import pandas as pd
from io import StringIO
import pymongo
from persiantools.jdatetime import JalaliDate
from persiantools import digits
import datetime
from Login import adminCheck
from sklearn.linear_model import LinearRegression
from Login import decrypt , encrypt, SendSms
import random
import requests
from Login import VerificationPhone , decrypt, encrypt
from ast import literal_eval
from flask import send_file
from PIL import Image, ImageDraw, ImageFont
import arabic_reshaper
from bidi.algorithm import get_display
import Fnc
from bson import ObjectId
from GuardPyCaptcha.Captch import GuardPyCaptcha
import numpy as np
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

def applynationalcode(data):
    captchas = GuardPyCaptcha()
    captchas = captchas.check_response(data['captchaCode'],data['UserInput']['captcha'])
    if captchas == False:
        return json.dumps({'replay':False,'msg':'کد تصویر صحیح نیست'})
    registerNoBours = farasahmDb['registerNoBours'].find_one({'کد ملی':data['UserInput']['nationalCode']},sort=[('date', -1)])
    if registerNoBours != None:
        VerificationPhone(registerNoBours['شماره تماس'])
        # phonPrivet = registerNoBours['شماره تماس']
        # phonPrivet = str(phonPrivet[:3])+'xxxxx'+str(phonPrivet[-3:])
        return json.dumps({'replay':True,'status':'Registered','phonPrivet':'xxxx'})
    return json.dumps({'replay':True,'status':'NotFund'})

def applycode(data):
    apply = farasahmDb['VerificationPhone'].find_one({'phone':data['inputPhone']['phone'],'code':str(data['inputPhone']['code'])})

    if apply != None:
        filter_query = {'nationalCode': data['inputPhone']['nationalCode']}
        update_query = {'$set': {'phone':data['inputPhone']['phone'],'nationalCode':data['inputPhone']['nationalCode'],'dateStart':datetime.datetime.now()}}
        farasahmDb['AuthenticationSession'].update_one(filter_query, update_query, upsert=True)
        idcode = encrypt(data['inputPhone']['nationalCode']).decode()
        return json.dumps({'replay':True,'id':idcode})
    else:
        return json.dumps({'replay':False,'msg':'کد تایید صحیح نیست'})






def coderegistered(data):
    registerNoBours = farasahmDb['registerNoBours'].find_one({'کد ملی':data['nationalCode']},sort=[('date', -1)], limit=1)
    if registerNoBours == None:
        return json.dumps({'replay':False,'msg':'کاربر یافت نشد'})
    phone = registerNoBours['شماره تماس']
    cheakcode = farasahmDb['VerificationPhone'].find_one({'phone':phone,'code':data['Code']})
    try:
        if cheakcode['code'] == None:
            return json.dumps({'replay':False,'msg':'کد تایید صحیح نیست'})
    except:
            return json.dumps({'replay':False,'msg':'کد تایید صحیح نیست'})
    id = str(registerNoBours['_id'])
    return json.dumps({'replay':True,'cookie':id})


def checkcookie(data):
    try:
        cookie = data['cookie']
        registerNoBours = farasahmDb['registerNoBours'].find_one({'_id':ObjectId(cookie)},{'_id':0})

        if registerNoBours == None:
            return json.dumps({'replay':False})
        registerNoBours['replay'] = True

        for key, value in registerNoBours.items():
            if isinstance(value, float) and np.isnan(value):
                registerNoBours[key] = ''
        return json.dumps(registerNoBours)
    except:
        return json.dumps({'replay':False})
    
def CookieToUser(data):
    try:
        cookie = data['cookie']
        registerNoBours = farasahmDb['registerNoBours'].find_one({'_id':ObjectId(cookie)},{'_id':0})

        if registerNoBours == None:
            return {'replay':False}
        registerNoBours['replay'] = True

        for key, value in registerNoBours.items():
            if isinstance(value, float) and np.isnan(value):
                registerNoBours[key] = ''
        return registerNoBours
    except:
        return {'replay':False}

def groupGetCompy(group):
    group = group[group['date']==group['date'].max()]
    return group

def getcompany(data):
    user = CookieToUser(data)
    if user['replay']==False:
        return json.dumps({'replay':False,'msg':'لطفا مجددا وارد شوید'})
    
    lastupDate = farasahmDb['register'].distinct('تاریخ گزارش')
    lastupDate = max(lastupDate)

    stockBourse = pd.DataFrame(farasahmDb['register'].find({"کد ملی": int(user['کد ملی']),'symbol':'visa','تاریخ گزارش':lastupDate},{'_id':0,'symbol':1,'سهام کل':1,'تاریخ گزارش':1}))

    if len(stockBourse)>0:
        stockBourse = stockBourse[stockBourse['تاریخ گزارش'] == stockBourse['تاریخ گزارش'].max()]
        stockBourse = stockBourse.drop_duplicates(subset=['symbol'])
        if len(stockBourse)>0:
            stockBourse = stockBourse[stockBourse['symbol']!='bazargam']
        stockBourse = stockBourse.rename(columns={'سهام کل':'تعداد سهام','تاریخ گزارش':'date'})
        listStock = stockBourse.to_dict('records')
    else:
        listStock = []

    stockNoBourse = pd.DataFrame(farasahmDb['registerNoBours'].find({"کد ملی": int(user['کد ملی'])},{'تعداد سهام':1,'_id':0,'date':1,'symbol':1}))
    if len(stockNoBourse)== 0:
        stockNoBourse = pd.DataFrame(farasahmDb['registerNoBours'].find({"کد ملی": str(user['کد ملی'])},{'تعداد سهام':1,'_id':0,'date':1,'symbol':1}))
    print('ff'*25)
    print(stockNoBourse)
    if len(stockNoBourse)>0:
        stockNoBourse = stockNoBourse[stockNoBourse['symbol']!='hevisa']
        stockNoBourse = stockNoBourse[stockNoBourse['symbol']!='yazdan']
        stockNoBourse = stockNoBourse.groupby(by=['symbol']).apply(groupGetCompy)
        stockNoBourse = stockNoBourse.to_dict('records')
    else:
        stockNoBourse = []
    listStock = listStock + stockNoBourse
    allCompany = pd.DataFrame(farasahmDb['companyList'].find({},{'_id':0}))
    allStockCompany = pd.DataFrame(farasahmDb['companyBasicInformation'].find({},{'_id':0,'تعداد سهام':1,'symbol':1}))
    allStockCompany = allStockCompany[allStockCompany['symbol']!='bazargam']
    allStockCompany = allStockCompany.set_index('symbol')
    allStockCompany = allStockCompany.rename(columns={'تعداد سهام':'allStockCompany'})

    allCompany = allCompany.set_index('symbol')
    allCompany = allCompany.join(allStockCompany)
    allCompany = allCompany[allCompany.index != 'hevisa']
    allCompany = allCompany[allCompany.index != 'yazdan']
    allCompany['allStockCompany'] = allCompany['allStockCompany'].fillna(0)
    listStock = pd.DataFrame(listStock)
    listStock = listStock.set_index('symbol')
    df = allCompany.join(listStock)
    df = df.drop(columns='date')
    df = df.fillna(0)
    df = df.sort_values(by=['تعداد سهام'],ascending=False)
    df = df.rename(columns={'تعداد سهام':'amount'})
    df = df.reset_index()
    df['allStockCompany_alpha'] = df['allStockCompany'].apply(int)
    df['allStockCompany_alpha'] = df['allStockCompany_alpha'].apply(digits.to_word)
    df['amount_alpha'] = df['amount'].apply(int)
    df['amount_alpha'] = df['amount_alpha'].apply(digits.to_word)
    df = df.to_dict('records')
    return json.dumps({'replay':True,'df':df})


def gettrade(data):
    user = CookieToUser(data['cookie'])
    if user['replay']==False: return json.dumps({'replay':False,'msg':'لطفا مجددا وارد شوید'})
    user = user['user']
    symbol =  data['symbol']
    company = farasahmDb['companyList'].find_one(symbol)
    typeCompany = company['type']
    if typeCompany == 'Bourse':
        codeBourse = farasahmDb['register'].find_one({'کد ملی':int(user['nationalCode'])})['کد سهامداری']
        df = pd.DataFrame(farasahmDb['traders'].find({'کد':codeBourse, 'symbol':symbol['symbol']},
                                                      {'_id':0, 'کد':0, 'صدور':0, 'fullname':0, 'symbol':0}))
        if len(df)==0:
            return json.dumps({'replay': False, 'msg':'معامله ای یافت نشد'})
        df = df.sort_values('date', ascending = False)
        df = df.to_dict('records')
        return json.dumps({'replay':True, 'df':df,'type':'Bourse'})
    
    nametrader = farasahmDb['registerNoBours'].find_one({'کد ملی':user['nationalCode']})['نام و نام خانوادگی']
    sell = pd.DataFrame(farasahmDb['transactions'].find({'sell':nametrader, 'symbol':symbol['symbol']},
                                                       {'_id':0,'id':0, 'buy':0, 'sell':0, 'symbol':0}))
    buy = pd.DataFrame(farasahmDb['transactions'].find({'buy':nametrader, 'symbol':symbol['symbol']},
                                                       {'_id':0,'id':0, 'sell':0, 'buy':0, 'symbol':0}))
    if len(sell) == 0 and len(buy) == 0:
        return json.dumps({'replay': False, 'msg': 'معامله ای یافت نشد'})   
    df = pd.concat([sell, buy])
    df = df.to_dict('records')
    return json.dumps({'replay': True, 'df':df, 'type':'NoBourse'})



def getsheet(data):
    user = CookieToUser(data)
    if user['replay']==False:
        return json.dumps({'replay':False,'msg':'لطفا مجددا وارد شوید'})

    symbol = data['symbol']
    company = farasahmDb['companyList'].find_one({'symbol':symbol})
    typeCompany = company['type']
    if typeCompany == 'Bourse':
        lastTrade = farasahmDb['trade'].find({'symbol':symbol['symbol']},{"تاریخ معامله":1})
        lastTrade =[x['تاریخ معامله'] for x in lastTrade]
        lastTrade = max(lastTrade)
        BourseUser = farasahmDb['register'].find_one({'کد ملی': int(user['کد ملی']), 'symbol': symbol['symbol'],'تاریخ گزارش': lastTrade},{'_id':0, 'fullName':1, 'نام پدر':1, 'کد ملی':1,'سهام کل':1})
        if BourseUser == None:
            return json.dumps({'replay': False, 'msg':'سهامی ندارید'})
        BourseUser['stockword'] = digits.to_word(BourseUser['سهام کل'])
        BourseUser['company'] = company['fullname']
        return json.dumps({'replay': True, 'sheet': BourseUser})
    
    lastDate = max(farasahmDb['registerNoBours'].distinct('date'))
    userNoBourse = farasahmDb['registerNoBours'].find_one({'کد ملی': user['کد ملی'], 'symbol':symbol, 'date':lastDate})
    if userNoBourse == None:
        userNoBourse = farasahmDb['registerNoBours'].find_one({'کد ملی': int(user['کد ملی']), 'symbol':symbol, 'date':lastDate})
    userNoBourse['stockword'] = digits.to_word(userNoBourse['تعداد سهام']) 
    userNoBourse['company'] = company['fullname']
    userNoBourse['fullName'] = userNoBourse['نام و نام خانوادگی']
    userNoBourse['سهام کل'] = userNoBourse['تعداد سهام']
    del userNoBourse['_id']
    for i in userNoBourse:
        userNoBourse[i] = str(userNoBourse[i])
    return json.dumps({'replay': True, 'sheet': userNoBourse})

def getassembly(data):
    user = CookieToUser(data)
    if user['replay']==False: return json.dumps({'replay':False,'msg':'لطفا مجددا وارد شوید'})
    user = user
    symbol = data['symbol']
    assembly = farasahmDb['assembly'].find_one({'symbol':symbol['symbol']}, sort = [('data', -1)])
    if assembly == None:
        return json.dumps({'replay':False, 'msg': 'مجمعی یافت نشد'})
    if assembly['date'] < datetime.datetime.now():
        return json.dumps({'replay':False, 'msg':'تاریخ مجمع پایان یافته است'})
    del assembly['_id']
    assembly['date_jalali'] = str(JalaliDate.to_jalali(assembly['date'].year, assembly['date'].month, assembly['date'].day))
    assembly['date'] = str(assembly['date'])
    return json.dumps({'replay':True, 'assembly': assembly})

def static(data):
    user = CookieToUser(data)
    if user['replay']==False:
        return json.dumps({'replay':False,'msg':'لطفا مجددا وارد شوید'})
    symbol = data['symbol']
    df_comp = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':symbol}))
    df_comp = df_comp.fillna('')
    df_comp = df_comp.sort_values(by=['date'])
    df_comp = df_comp.drop_duplicates(subset=['date','کد ملی'],keep='last')
    
    if len(df_comp)==0:
        return json.dumps({'replay':False, 'msg': 'not found'}) 
    df_comp = df_comp[df_comp['date']==df_comp['date'].max()]
    
    Shareholders = len(df_comp)
    number_shares = df_comp['تعداد سهام'].sum()
    Shareholder = farasahmDb['registerNoBours'].find_one({'symbol':symbol,'کد ملی':user['کد ملی']})
    amount = Shareholder['تعداد سهام']
    capital = number_shares * 10000
    dic = {'Shareholders':Shareholders,'number_shares':str(number_shares),'amount':amount, 'capital':str(capital)}
    return json.dumps({'replay':True, 'dic': dic}) 


def getSheetpng(data):
    resulte = json.loads(getsheet(data))

    if resulte['replay'] == False:
        return getsheet(data)
    
    resulte = resulte['sheet']
    companyInfo = farasahmDb['companyBasicInformation'].find_one({'symbol':resulte['symbol']})
    image = Image.open('public/sheet1.png')
    image_width, image_height = image.size
    draw = ImageDraw.Draw(image)
    font120 = ImageFont.truetype('public/Peyda-Medium.ttf', 120)
    font60 = ImageFont.truetype('public/Peyda-Medium.ttf', 60)
    font40 = ImageFont.truetype('public/Peyda-Medium.ttf', 40)
    font50 = ImageFont.truetype('public/Peyda-Medium.ttf', 50)

    # اسم شرکت
    text = arabic_reshaper.reshape('شرکت ' + resulte['company'])
    text_width = draw.textlength(text, font=font120)
    x = (image_width - text_width) // 2
    y = 350
    text = get_display(text)
    draw.text((x, y), text, fill=(100,50,25), font=font120)
    # سهامی خاص

    text = arabic_reshaper.reshape('('+companyInfo['نوع شرکت']+')')
    text_width = draw.textlength(text, font=font60)
    x = (image_width - text_width) // 2
    y = 500
    text = get_display(text)
    draw.text((x, y), text, fill=(115,105,95), font=font60)

    # شماره و تاریخ ثبت
    text = arabic_reshaper.reshape('شماره ثبت:'+digits.en_to_fa(companyInfo['شماره ثبت'])+ '       ' +'تاریخ تاسیس:' + digits.en_to_fa(companyInfo['تاریخ تاسیس']))
    text_width = draw.textlength(text, font=font40)
    x = (image_width - text_width) // 2
    y = 650
    text = get_display(text)
    draw.text((x, y), text, fill=(10,10,35), font=font40)
    #سرمایه ثبتی

    text = arabic_reshaper.reshape('سرمایه ثبت شده '+Fnc.comma_separate(digits.en_to_fa(companyInfo['سرمایه ثبتی']))+ ' ريال منقسم به '+ Fnc.comma_separate(digits.en_to_fa(companyInfo['تعداد سهام'])) + ' سهم ' + digits.to_word(int((int(companyInfo['سرمایه ثبتی']) / int(companyInfo['تعداد سهام'])))) + ' ريالی  که صد در دصد آن پرداخت شده میباشد')
    text_width = draw.textlength(text, font=font40)
    x = (image_width - text_width) // 2
    y = 740
    text = get_display(text)
    draw.text((x, y), text, fill=(10,10,35), font=font40)

    #فیلد ها دارنده این ورقه
    text = arabic_reshaper.reshape('دارنده این ورقه' + '      ' + resulte['نام و نام خانوادگی'])
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 280 
    y = 1100
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #فیلد ها فرزند
    text = arabic_reshaper.reshape('فرزند' + '      ' + resulte['نام پدر'])
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 1400 
    y = 1100
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #فیلد ها کد ملی
    text = arabic_reshaper.reshape('شماره/شناسه ملی' + '      ' + digits.en_to_fa(resulte['کد ملی']))
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 2100 
    y = 1100
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #فیلد ها تعداد سهام
    text = arabic_reshaper.reshape('مالک تعداد' + '      ' + Fnc.comma_separate(digits.en_to_fa(str(resulte['تعداد سهام']))) + '      (' + digits.to_word(int(resulte['تعداد سهام']))+')' + '       سهم ' + digits.to_word(int((int(companyInfo['سرمایه ثبتی']) / int(companyInfo['تعداد سهام'])))) + '    ريالی با نام')
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 280 
    y = 1200
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #فیلد ها تعداد سهام
    text = arabic_reshaper.reshape('از شرکت    ' + resulte['company'] + ' میباشد.')
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 280 
    y = 1300
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #حقوق مشخصه
    text = arabic_reshaper.reshape('مالک سهام دارای حقوق مشخصه در اساسنامه شرکت میباشد.')
    text_width = draw.textlength(text, font=font40)
    x = (image_width - text_width) // 2
    y = 1450
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)

    #مدیرعامل
    text = arabic_reshaper.reshape('مدیرعامل')
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*3
    y = 1800
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)
    #اسم مدیرعامل
    text = arabic_reshaper.reshape(companyInfo['مدیر عامل'])
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*3
    y = 1900
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)

    #هیئت مدیره
    text = arabic_reshaper.reshape('رئیس هیئت مدیره')
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*1
    y = 1800
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)
    #اسم هیئت مدیره
    text = arabic_reshaper.reshape(companyInfo['رئیس هیئت مدیره'])
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*1
    y = 1900
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)

    #مهر شرکت
    text = arabic_reshaper.reshape('مهر شرکت')
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*2
    y = 1800
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)
    image.save('public/sheet1_download.png')
    return send_file("public/sheet1_download.png", as_attachment=True, mimetype="image/png")


def getSheetpngAdmin(data):
    access = data['access'][0]
    symbol = data['access'][1]
    company = farasahmDb['companyList'].find_one({'symbol':symbol})
    _id= ObjectId(access)
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc == None:
        return json.dumps({'reply':False,'msg':'کاربر یافت نشد لطفا مجددا وارد شوید'})
    resulte = farasahmDb['registerNoBours'].find_one({'کد ملی': data['inp'], 'symbol':symbol},sort=[('date',-1)])
    companyInfo = farasahmDb['companyBasicInformation'].find_one({'symbol':symbol})
    image = Image.open('public/sheet1.png')
    image_width, image_height = image.size
    draw = ImageDraw.Draw(image)
    font120 = ImageFont.truetype('public/Peyda-Medium.ttf', 120)
    font60 = ImageFont.truetype('public/Peyda-Medium.ttf', 60)
    font40 = ImageFont.truetype('public/Peyda-Medium.ttf', 40)
    font50 = ImageFont.truetype('public/Peyda-Medium.ttf', 50)

    # اسم شرکت
    text = arabic_reshaper.reshape('شرکت ' + company['fullname'])
    text_width = draw.textlength(text, font=font120)
    x = (image_width - text_width) // 2
    y = 350
    text = get_display(text)
    draw.text((x, y), text, fill=(100,50,25), font=font120)
    # سهامی خاص

    text = arabic_reshaper.reshape('('+companyInfo['نوع شرکت']+')')
    text_width = draw.textlength(text, font=font60)
    x = (image_width - text_width) // 2
    y = 500
    text = get_display(text)
    draw.text((x, y), text, fill=(115,105,95), font=font60)

    # شماره و تاریخ ثبت
    text = arabic_reshaper.reshape('شماره ثبت:'+digits.en_to_fa(companyInfo['شماره ثبت'])+ '       ' +'تاریخ تاسیس:' + digits.en_to_fa(companyInfo['تاریخ تاسیس']))
    text_width = draw.textlength(text, font=font40)
    x = (image_width - text_width) // 2
    y = 650
    text = get_display(text)
    draw.text((x, y), text, fill=(10,10,35), font=font40)
    #سرمایه ثبتی

    text = arabic_reshaper.reshape('سرمایه ثبت شده '+Fnc.comma_separate(digits.en_to_fa(companyInfo['سرمایه ثبتی']))+ ' ريال منقسم به '+ Fnc.comma_separate(digits.en_to_fa(companyInfo['تعداد سهام'])) + ' سهم ' + digits.to_word(int((int(companyInfo['سرمایه ثبتی']) / int(companyInfo['تعداد سهام'])))) + ' ريالی  که صد در دصد آن پرداخت شده میباشد')
    text_width = draw.textlength(text, font=font40)
    x = (image_width - text_width) // 2
    y = 740
    text = get_display(text)
    draw.text((x, y), text, fill=(10,10,35), font=font40)

    #فیلد ها دارنده این ورقه
    text = arabic_reshaper.reshape('دارنده این ورقه' + '      ' + resulte['نام و نام خانوادگی'])
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 280 
    y = 1100
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #فیلد ها فرزند
    try:
        text = arabic_reshaper.reshape('فرزند' + '      ' + resulte['نام پدر'])
    except:
        text = arabic_reshaper.reshape('فرزند' + '      ')
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 1400 
    y = 1100
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #فیلد ها کد ملی
    text = arabic_reshaper.reshape('شماره/شناسه ملی' + '      ' + digits.en_to_fa(resulte['کد ملی']))
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 2100 
    y = 1100
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #فیلد ها تعداد سهام
    text = 'مالک تعداد' + '      ' + Fnc.comma_separate(digits.en_to_fa(str(resulte['تعداد سهام'])))
    text = text + '      (' + digits.to_word(int(resulte['تعداد سهام'])) +')' + '       سهم '
    text = text + digits.to_word(int((int(companyInfo['سرمایه ثبتی']) / int(companyInfo['تعداد سهام'])))) + '    ريالی با نام از شرکت    '
    text = text + company['fullname'] + ' میباشد.'
    text = arabic_reshaper.reshape(text)
    text_width = draw.textlength(text, font=font50)
    x = image_width - text_width - 280 
    y = 1200
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font50)

    #حقوق مشخصه
    text = arabic_reshaper.reshape('مالک سهام دارای حقوق مشخصه در اساسنامه شرکت میباشد.')
    text_width = draw.textlength(text, font=font40)
    x = (image_width - text_width) // 2
    y = 1400
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)

    #مدیرعامل
    text = arabic_reshaper.reshape('مدیرعامل')
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*3
    y = 1800
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)
    #اسم مدیرعامل
    text = arabic_reshaper.reshape(companyInfo['مدیر عامل'])
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*3
    y = 1900
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)

    #هیئت مدیره
    text = arabic_reshaper.reshape('رئیس هیئت مدیره')
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*1
    y = 1800
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)
    #اسم هیئت مدیره
    text = arabic_reshaper.reshape(companyInfo['رئیس هیئت مدیره'])
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*1
    y = 1900
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)

    #مهر شرکت
    text = arabic_reshaper.reshape('مهر شرکت')
    text_width = draw.textlength(text, font=font40)
    x = ((image_width - text_width) // 4)*2
    y = 1800
    text = get_display(text)
    draw.text((x, y), text, fill=(0,0,0), font=font40)
    image.save('public/sheet1_download.png')
    return send_file("public/sheet1_download.png", as_attachment=True, mimetype="image/png")

