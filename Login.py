import pymongo
from cryptography.fernet import Fernet
import json
from bson import ObjectId
import random
import json
import requests
import random
from GuardPyCaptcha.Captch import GuardPyCaptcha
import datetime
key = 'KPms1b_Kibq5XR6M0d88rJTsjjgdlBFzbFN4irIxiHo='
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

frm ='30001526'
usrnm = 'isatispooya'
psswrd ='5246043adeleh'

def SendSms(snd,txt):
    resp = requests.get(url=f'http://tsms.ir/url/tsmshttp.php?from={frm}&to={snd}&username={usrnm}&password={psswrd}&message={txt}').json()
    print(txt)
    return resp




def VerificationPhone(phone):
    code = str(random.randint(10000,99999))
    phoneEdite = str(phone)
    if '.' in phoneEdite:
        phoneEdite = phoneEdite.split('.')[0]
        phoneEdite = int(phoneEdite)
    phoneEdite = '0' + str(phoneEdite)
    #farasahmDb['VerificationPhone'].delete_many({'phone':phone})
    farasahmDb['VerificationPhone'].insert_one({'phone':phone,'code':code,'datetime':datetime.datetime.now()})
    text = 'کد تایید فراسهم \n' + str(code)
    res = SendSms(phoneEdite,text)
    return json.dumps({'replay':True})




f = Fernet(key)

def encrypt(msg):
    msg = str(msg).encode()
    msg = f.encrypt(msg)
    return msg

def decrypt(msg):
    msg = f.decrypt(msg)
    msg = msg.decode()
    return msg

    

def captcha():
    captchas = GuardPyCaptcha()
    captchas = captchas.Captcha_generation()
    return json.dumps(captchas)

def applyPhone(data):
    captchas = GuardPyCaptcha()
    try : 
    
        captchas = captchas.check_response(data['captchaCode'],data['inputPhone']['captcha'])
    except:
        
        return json.dumps({'replay':False,'msg':'کد تصویر صحیح نیست'})
    if captchas == False:
        return json.dumps({'replay':False,'msg':'کد تصویر صحیح نیست'})
    if farasahmDb['user'].find_one({'phone':data['inputPhone']['phone']})==None:
        return json.dumps({'replay':False,'msg':'شماره همراه صحیح نیست'})
    return VerificationPhone(data['inputPhone']['phone'])

def applyCode(data):
    apply = farasahmDb['VerificationPhone'].find_one({'phone':data['inputPhone']['phone'],'code':str(data['inputPhone']['code'])})
    if apply == None:
        return json.dumps({'replay':False,'msg':'کد تایید صحیح نیست'})
    id = str(farasahmDb['user'].find_one({'phone':data['inputPhone']['phone']})['_id'])
    return json.dumps({'replay':True,'id':id})



def access(data):
    _id= ObjectId(data['id'])
    acc = farasahmDb['user'].find_one({'_id':_id},{'_id':0})
    if acc==None:
        return json.dumps({'replay':False})
    menu = [x for x in farasahmDb['menu'].find({},{'_id':0})]
    enabled = []
    disabled = []
    if acc['enabled']['all'] == True:
        enabled = menu
    else:
        for i in menu:
            if i['name'] in acc['enabled'].keys():
                if acc['enabled'][i['name']]['all']:
                    enabled.append(i)
                else:
                    dicMenu = i
                    for j in i['menu']:
                        if j['url'] not in acc['enabled'][i['name']]['mainMenu']:
                            dicMenu['menu'].remove(j)
                    enabled.append(dicMenu)
            elif i['name'] in acc['disabled']:
                disabled.append(i)
    del acc['enabled']
    del acc['disabled']
    return json.dumps({'replay':True,'acc':acc, 'enabled':enabled, 'disabled':disabled})

def adminCheck(id):
    _id= ObjectId(id)
    acc = farasahmDb['user'].find_one({'_id':_id},{'admin':1})
    if acc==None:
        return False
    return acc['admin']


def getApp(data):
    _id= ObjectId(data['id'])
    acc = farasahmDb['user'].find_one({'_id':_id})
    if acc==None:
        return json.dumps({'reply':False})
    app = farasahmDb['menu'].find_one({'name':data['symbol']},{'_id':0})
    if app==None:
        return json.dumps({'reply':False})
    if acc['enabled']['all'] == True:
        return json.dumps({'reply':True, 'app':app})
    else:
        if acc['enabled'][data['symbol']]['all']:
            return json.dumps({'reply':True, 'app':app})
        else:
            appMenu = []
            for item in app['menu']:
                if item['url'] in acc['enabled'][data['symbol']]['mainMenu']:
                    if len(item['sub'])==0:
                        appMenu.append(item)
                    else:
                        appSubMenu = []
                        for j in item['sub']:
                            if j['url'] in acc['enabled'][data['symbol']]['mainMenu']:
                                appSubMenu.append(j)
                        if len(appSubMenu)>0:
                            item['sub'] = appSubMenu
                            appMenu.append(item)
            app['menu'] = appMenu
    return json.dumps({'reply':True, 'app':app})