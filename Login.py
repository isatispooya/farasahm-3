import pymongo
from cryptography.fernet import Fernet
import json
from bson import ObjectId
import cv2
import base64
import random
import numpy as np
import string

client = pymongo.MongoClient()
farasahmDb = client['farasahm2']

import json
import requests
import random

frm ='30001526'
usrnm = 'isatispooya'
psswrd ='5246043adeleh'

def SendSms(snd,txt):
    resp = requests.get(url=f'http://tsms.ir/url/tsmshttp.php?from={frm}&to={snd}&username={usrnm}&password={psswrd}&message={txt}').json()
    print(txt)
    return resp


def VerificationPhone(phone):
    code = str(random.randint(10000,99999))
    farasahmDb['VerificationPhone'].delete_many({'phone':phone})
    farasahmDb['VerificationPhone'].insert_one({'phone':phone,'code':code})
    text = 'کد تایید فراسهم \n' + str(code)
    res = SendSms(phone,text)
    return json.dumps({'replay':True})



def captchaGenerate():
    font = cv2.FONT_HERSHEY_COMPLEX
    captcha = np.zeros((50,250,3), np.uint8)
    captcha[:] = (234, 228, 228)#(random.randint(235,255),random.randint(245,255),random.randint(245,255))
    font= cv2.FONT_HERSHEY_SIMPLEX
    texcode = ''
    listCharector =  string.digits+string.ascii_lowercase+string.digits
    for i in range(1,5):
        bottomLeftCornerOfText = (random.randint(35,45)*i,35+(random.randint(-8,8)))
        fontScale= random.randint(7,15)/10
        fontColor= (random.randint(0,180),random.randint(0,180),random.randint(0,180))
        thickness= random.randint(1,2)
        lineType= 1
        text = str(listCharector[random.randint(0,len(listCharector)-1)])
        texcode = texcode+(text)
        cv2.putText(captcha,text,bottomLeftCornerOfText,font,fontScale,fontColor,thickness,lineType)
        if random.randint(0,2)>0:
            pt1 = (random.randint(0,250),random.randint(0,50))
            pt2 = (random.randint(0,250),random.randint(0,50))
            lineColor = (random.randint(0,150),random.randint(0,150),random.randint(0,150))
            cv2.line(captcha,pt1,pt2,lineColor,1)
    address = 'C:\\Users\\moeen\\Desktop\\project\\pishkar\\Front\\pishkar\\public\\captcha\\'+texcode+'.jpg'
    stringImg = base64.b64encode(cv2.imencode('.jpg', captcha)[1]).decode()
    return [texcode,stringImg]

from cryptography.fernet import Fernet


key = 'KPms1b_Kibq5XR6M0d88rJTsjjgdlBFzbFN4irIxiHo='
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
    cg = captchaGenerate()
    return json.dumps({'captcha':str(encrypt(cg[0])),'img':cg[1]})

def applyPhone(data):
    textCaptcha = decrypt(data['captchaCode'][2:-1].encode())
    if textCaptcha!=data['inputPhone']['captcha']:
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