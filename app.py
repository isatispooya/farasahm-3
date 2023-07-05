
from flask import Flask, request
import json
from flask_cors import CORS
import pymongo
import warnings
import datetime
import random
from cryptography.fernet import Fernet
import winreg as reg
from waitress import serve
import dataManagment
import Login
import report
import dara

def addToReg(): 
    key = reg.OpenKey(reg.HKEY_CURRENT_USER , "Software\Microsoft\Windows\CurrentVersion\Run" ,0 , reg.KEY_ALL_ACCESS) # Open The Key
    reg.SetValueEx(key ,"any_name" , 0 , reg.REG_SZ , __file__) # Appending Script Address
    reg.CloseKey(key) 
addToReg()
warnings.filterwarnings("ignore")

client = pymongo.MongoClient()
farasahmDb = client['farasahm2']
userCl = farasahmDb['user']

app = Flask(__name__)
CORS(app)

@app.route('/captcha',methods = ['POST', 'GET'])
def captcha():
    return Login.captcha()

@app.route('/applyphone',methods = ['POST', 'GET'])
def applyPhone():
    data = request.get_json()
    return Login.applyPhone(data)

@app.route('/applycode',methods = ['POST', 'GET'])
def applyCode():
    data = request.get_json()
    return Login.applyCode(data)

@app.route('/access',methods = ['POST', 'GET'])
def access():
    data = request.get_json()
    return Login.access(data)

@app.route('/update',methods = ['POST', 'GET'])
def update():
    access = request.form['access']
    daily =  request.files['daily']
    registerdaily =  request.files['registerdaily']
    return dataManagment.Update(access,daily,registerdaily)

@app.route('/createtraders',methods = ['POST', 'GET'])
def createTraders():
    data = request.get_json()
    return dataManagment.createTraders(data)

@app.route('/createnewtraders',methods = ['POST', 'GET'])
def createnewtraders():
    data = request.get_json()
    return dataManagment.createNewTraders(data)

@app.route('/createstation',methods = ['POST', 'GET'])
def createstation():
    data = request.get_json()
    return dataManagment.createStation(data)

@app.route('/createholder',methods = ['POST', 'GET'])
def createholder():
    data = request.get_json()
    return dataManagment.createholder(data)

@app.route('/createbroker',methods = ['POST', 'GET'])
def createBroker():
    data = request.get_json()
    return dataManagment.createBroker(data)

@app.route('/mashinlearninimg',methods = ['POST', 'GET'])
def mashinlearninimg():
    data = request.get_json()
    return dataManagment.mashinlearninimg(data)

@app.route('/lastupdate',methods = ['POST', 'GET'])
def lastupdate():
    data = request.get_json()
    return dataManagment.lastupdate(data)

@app.route('/gettoptraders',methods = ['POST', 'GET'])
def gettoptraders():
    data = request.get_json()
    return report.gettoptraders(data)

@app.route('/gettopbroker',methods = ['POST', 'GET'])
def gettopbroker():
    data = request.get_json()
    return report.gettopbroker(data)

@app.route('/getnewcomer',methods = ['POST', 'GET'])
def getnewcomer():
    data = request.get_json()
    return report.getnewcomer(data)

@app.route('/getdftraders',methods = ['POST', 'GET'])
def getdftraders():
    data = request.get_json()
    return report.getdftraders(data)

@app.route('/getnewcomerall',methods = ['POST', 'GET'])
def getnewcomerall():
    data = request.get_json()
    return report.getnewcomerall(data)

@app.route('/getstation',methods = ['POST', 'GET'])
def getstation():
    data = request.get_json()
    return report.getstation(data)

@app.route('/getbroker',methods = ['POST', 'GET'])
def getbroker():
    data = request.get_json()
    return report.getbroker(data)


@app.route('/getstockman',methods = ['POST', 'GET'])
def getstockman():
    data = request.get_json()
    return report.getstockman(data)

@app.route('/getallname',methods = ['POST', 'GET'])
def getallname():
    data = request.get_json()
    return report.getallname(data)

@app.route('/getallnamenobourse',methods = ['POST', 'GET'])
def getallnameNoBours():
    data = request.get_json()
    return report.getallnameNoBours(data)

@app.route('/getallnameplus',methods = ['POST', 'GET'])
def getallnameplus():
    data = request.get_json()
    return report.getallnameplus(data)

@app.route('/getdetailstrade',methods = ['POST', 'GET'])
def getdetailstrade():
    data = request.get_json()
    return report.getdetailstrade(data)


@app.route('/getnav',methods = ['POST', 'GET'])
def getnav():
    data = request.get_json()
    return report.getnav(data)

@app.route('/getreturn',methods = ['POST', 'GET'])
def getreturn():
    data = request.get_json()
    return report.getreturn(data)

@app.route('/getcompare',methods = ['POST', 'GET'])
def getcompare():
    data = request.get_json()
    return report.getcompare(data)

@app.route('/getshareholders',methods = ['POST', 'GET'])
def getshareholders():
    data = request.get_json()
    return report.getshareholders(data)

@app.route('/setgrouping',methods = ['POST', 'GET'])
def setgrouping():
    data = request.get_json()
    return dataManagment.setgrouping(data)

@app.route('/getgrouping',methods = ['POST', 'GET'])
def getgrouping():
    data = request.get_json()
    return report.getgrouping(data)

@app.route('/delrowgrouping',methods = ['POST', 'GET'])
def delrowgrouping():
    data = request.get_json()
    return dataManagment.delrowgrouping(data)


@app.route('/getprofiletrader',methods = ['POST', 'GET'])
def getprofiletrader():
    data = request.get_json()
    return report.getprofiletrader(data)

@app.route('/getbalancetrader',methods = ['POST', 'GET'])
def getbalancetrader():
    data = request.get_json()
    return report.getbalancetrader(data)

@app.route('/getpersonalnobourse',methods = ['POST', 'GET'])
def getpersonalnobourse():
    data = request.get_json()
    return report.getpersonalnobourse(data)

@app.route('/deltransaction',methods = ['POST', 'GET'])
def deltransaction():
    data = request.get_json()
    return dataManagment.deltransaction(data)

@app.route('/settransaction',methods = ['POST', 'GET'])
def setTransaction():
    data = request.get_json()
    return dataManagment.setTransaction(data)


@app.route('/gettransactions',methods = ['POST', 'GET'])
def gettransactions():
    data = request.get_json()
    return report.gettransactions(data)


@app.route('/addtradernobourse',methods = ['POST', 'GET'])
def addtradernobourse():
    data = request.get_json()
    return dataManagment.addtradernobourse(data)

@app.route('/delshareholders',methods = ['POST', 'GET'])
def delshareholders():
    data = request.get_json()
    return dataManagment.delshareholders(data)

@app.route('/getinformationcompany',methods = ['POST', 'GET'])
def getinformationcompany():
    data = request.get_json()
    return report.getinformationcompany(data)

@app.route('/setinformationcompany',methods = ['POST', 'GET'])
def setinformationcompany():
    data = request.get_json()
    return dataManagment.setinformationcompany(data)

@app.route('/gettraderactivityreport',methods = ['POST', 'GET'])
def gettraderactivityreport():
    data = request.get_json()
    return report.gettraderactivityreport(data)

@app.route('/getbrokeractivityreport',methods = ['POST', 'GET'])
def getbrokeractivityreport():
    data = request.get_json()
    return report.getbrokeractivityreport(data)

@app.route('/getstationactivityreport',methods = ['POST', 'GET'])
def getstationactivityreport():
    data = request.get_json()
    return report.getstationactivityreport(data)

@app.route('/excerpttrader',methods = ['POST', 'GET'])
def excerpttrader():
    data = request.get_json()
    return report.excerpttrader(data)

@app.route('/syncboursi',methods = ['POST', 'GET'])
def syncBoursi():
    data = request.get_json()
    return dataManagment.syncBoursi(data)

@app.route('/syncbook',methods = ['POST', 'GET'])
def syncbook():
    data = request.get_json()
    return dataManagment.syncbook(data)

@app.route('/getformerstockman',methods = ['POST', 'GET'])
def getformerstockman():
    data = request.get_json()
    return report.getformerstockman(data)

@app.route('/getreportmetric',methods = ['POST', 'GET'])
def getreportmetric():
    data = request.get_json()
    return report.getreportmetric(data)

@app.route('/createassembly',methods = ['POST', 'GET'])
def createassembly():
    data = request.get_json()
    return dataManagment.createassembly(data)

@app.route('/delassembly',methods = ['POST', 'GET'])
def delassembly():
    data = request.get_json()
    return dataManagment.delassembly(data)


@app.route('/getassembly',methods = ['POST', 'GET'])
def mgetassembly():
    data = request.get_json()
    return report.getassembly(data)


@app.route('/dara/applynationalcode',methods = ['POST', 'GET'])
def dara_applynationalcode():
    data = request.get_json()
    return dara.applynationalcode(data)

@app.route('/dara/questionauth',methods = ['POST', 'GET'])
def dara_questionauth():
    data = request.get_json()
    return dara.questionauth(data)

@app.route('/dara/answerauth',methods = ['POST', 'GET'])
def dara_answerauth():
    data = request.get_json()
    return dara.answerauth(data)

@app.route('/dara/register',methods = ['POST', 'GET'])
def dara_register():
    data = request.get_json()
    return dara.register(data)

@app.route('/dara/gettrade', methods =['POST', 'GET'])
def dara_gettrade():
    data = request.get_json()
    return dara.gettrade(data)


@app.route('/dara/coderegister',methods = ['POST', 'GET'])
def dara_coderegister():
    data = request.get_json()
    return dara.codeRegister(data)

@app.route('/dara/coderegistered',methods = ['POST', 'GET'])
def dara_coderegistered():
    data = request.get_json()
    return dara.coderegistered(data)

@app.route('/dara/checkcookie',methods = ['POST', 'GET'])
def dara_checkcookie():
    data = request.get_json()
    return dara.checkcookie(data)

@app.route('/dara/applycode',methods = ['POST', 'GET'])
def dara_applycode():
    data = request.get_json()
    return dara.applycode(data)

@app.route('/dara/access',methods = ['POST', 'GET'])
def dara_access():
    data = request.get_json()
    return dara.access(data)

@app.route('/dara/authenticationsession',methods = ['POST', 'GET'])
def dara_authenticationsession():
    data = request.get_json()
    return dara.authenticationsession(data)

@app.route('/dara/getcompany',methods = ['POST', 'GET'])
def dara_getcompany():
    data = request.get_json()
    return dara.getcompany(data)

@app.route('/dara/getassembly',methods = ['POST', 'GET'])
def getassembly():
    data = request.get_json()
    return dara.getassembly(data)

@app.route('/dara/getsheet',methods = ['POST', 'GET'])
def dara_getsheet():
    data = request.get_json()
    return dara.getsheet(data)

if __name__ == '__main__':
    #serve(app, host="0.0.0.0", port=8080,threads= 8)
    app.run(host='0.0.0.0', debug=True)