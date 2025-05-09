from flask import Flask, request
import json
from flask_cors import CORS
import warnings
import winreg as reg
from waitress import serve
import dataManagment
import Login
import report
import dara
import marketing
from setting import farasahmDb


def addToReg(): 
    key = reg.OpenKey(reg.HKEY_CURRENT_USER , "Software\Microsoft\Windows\CurrentVersion\Run" ,0 , reg.KEY_ALL_ACCESS)
    reg.SetValueEx(key ,"any_name" , 0 , reg.REG_SZ , __file__)
    reg.CloseKey(key)
    
addToReg()

warnings.filterwarnings("ignore")



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

@app.route('/getapp',methods = ['POST', 'GET'])
def getApp():
    data = request.get_json()
    return Login.getApp(data)


@app.route('/get_cookie_space', methods=['POST'])
def get_cookie_space():
    ApiKey = request.headers.get('X-API-Key')
    data = request.get_json()
    return Login.get_cookie_space(ApiKey, data)



@app.route('/access',methods = ['POST', 'GET'])
def access():
    data = request.get_json()
    return Login.access(data)

@app.route('/update',methods = ['POST', 'GET'])
def update():
    access = request.form['access']
    daily =  request.files['daily']
    registerdaily = request.files['registerdaily']
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

@app.route('/getrateassetfixincom',methods = ['POST', 'GET'])
def getrateassetfixincom():
    data = request.get_json()
    return report.getrateassetfixincom(data)

@app.route('/sendsmsgroup',methods = ['POST', 'GET'])
def smsgroup():
    data = request.get_json()
    return report.smsgroup(data)

@app.route('/smsgroupreport',methods = ['POST', 'GET'])
def smsgroupreport():
    data = request.get_json()
    return report.smsgroupreport(data)

@app.route('/getretrnprice',methods = ['POST', 'GET'])
def getretrnprice():
    data = request.get_json()
    return report.getretrnprice(data)

@app.route('/getonwerfix',methods = ['POST', 'GET'])
def getonwerfix():
    data = request.get_json()
    return report.getonwerfix(data)

@app.route('/getdiffnavprc',methods = ['POST', 'GET'])
def getdiffnavprc():
    data = request.get_json()
    return report.getdiffnavprc(data)

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

@app.route('/getdiffassetamarydash',methods = ['POST', 'GET'])
def getdiffassetamarydash():
    data = request.get_json()
    return report.getdiffassetamarydash(data)

@app.route('/getpotentialcoustomer',methods = ['POST', 'GET'])
def getpotentialcoustomer():
    data = request.get_json()
    return report.getpotentialcoustomer(data)

@app.route('/dashpotantialsymbol',methods = ['POST', 'GET'])
def dashpotantialsymbol():
    data = request.get_json()
    return report.dashpotantialsymbol(data)

@app.route('/getreturnasset',methods = ['POST', 'GET'])
def getreturnasset():
    data = request.get_json()
    return report.getreturnasset(data)

@app.route('/staticownerincomp',methods = ['POST', 'GET'])
def staticownerincomp():
    data = request.get_json()
    return report.staticownerincomp(data)

@app.route('/saverateretasst',methods = ['POST', 'GET'])
def saverateretasst():
    data = request.get_json()
    return report.saverateretasst(data)

@app.route('/getcompare',methods = ['POST', 'GET'])
def getcompare():
    data = request.get_json()
    return report.getcompare(data)

@app.route('/calcincass',methods = ['POST', 'GET'])
def calcincass():
    data = request.get_json()
    return report.calcincass(data)

@app.route('/getretassfix',methods = ['POST', 'GET'])
def getretassfix():
    data = request.get_json()
    return report.getretassfix(data)

@app.route('/getoraghytm',methods = ['POST', 'GET'])
def getoraghytm():
    data = request.get_json()
    return report.getoraghytm(data)

@app.route('/getpriceforward',methods = ['POST', 'GET'])
def getpriceforward():
    data = request.get_json()
    return report.getpriceforward(data)

@app.route('/addcompany',methods = ['POST', 'GET'])
def addcompany():
    if 'key' not in request.files:
        return json.dumps({'reply': False,'msg':'کلید یافت نشد'})
    key = request.files['key']
    if key.filename == '':
        return json.dumps({'reply': False,'msg':'کلید یافت نشد'})
    name = request.form.get('name', '')
    idTax = request.form.get('idTax', '')
    idNum = request.form.get('idNum', '')
    access = request.form.get('access', '')
    address = request.form.get('address', '')
    call = request.form.get('call', '')
    postcode = request.form.get('postcode', '')
    return report.addcompany(access, key, name, idTax, idNum, address, call, postcode)

@app.route('/getcompanymoadian',methods = ['POST', 'GET'])
def getcompanymoadian():
    data = request.get_json()
    return report.getcompanymoadian(data)

@app.route('/moadian/getinvoice',methods = ['POST', 'GET'])
def moadian_getinvoice():
    data = request.get_json()
    return report.moadian_getinvoice(data)

@app.route('/moadian/getSellerMoadian',methods = ['POST', 'GET'])
def moadian_getSellerMoadian():
    data = request.get_json()
    return report.getSellerMoadian(data)

@app.route('/moadian/detail',methods = ['POST', 'GET'])
def moadian_detail():
    data = request.get_json()
    return report.Moadiandetail(data)


@app.route('/moadian/cloninvoiceebtal',methods = ['POST', 'GET'])
def moadian_cloninvoiceebtal():
    data = request.get_json()
    return report.ebtalinvoce(data)

@app.route('/moadian/delinvoice',methods = ['POST', 'GET'])
def moadian_delinvoice():
    data = request.get_json()
    return report.moadian_delinvoice(data)

@app.route('/moadian/print',methods = ['POST', 'GET'])
def moadian_print():
    data = request.get_json()
    return report.moadian_print(data)

@app.route('/delcompanymoadian',methods = ['POST', 'GET'])
def delcompanymoadian():
    data = request.get_json()
    return report.delcompanymoadian(data)

@app.route('/getinvoce',methods = ['POST', 'GET'])
def getinvoce():
    data = request.get_json()
    return report.getinvoce(data)

@app.route('/inquiryinvoce',methods = ['POST', 'GET'])
def inquiryinvoce():
    data = request.get_json()
    return report.inquiryinvoce(data)

@app.route('/sendinvoce',methods = ['POST', 'GET'])
def sendinvoce():
    data = request.get_json()
    return report.sendinvoce(data)

@app.route('/moadian/cloninvoce',methods = ['POST', 'GET'])
def cloninvoce():
    data = request.get_json()
    return report.cloninvoce(data)

@app.route('/getlistcompanymoadian',methods = ['POST', 'GET'])
def getlistcompanymoadian():
    data = request.get_json()
    return report.getlistcompanymoadian(data)

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


@app.route('/getdatepriority',methods = ['POST', 'GET'])
def getdatepriority():
    data = request.get_json()
    return dataManagment.getdatepriority(data)

@app.route('/getprofiletrader',methods = ['POST', 'GET'])
def getprofiletrader():
    data = request.get_json()
    return report.getprofiletrader(data)

@app.route('/getbalancetrader',methods = ['POST', 'GET'])
def getbalancetrader():
    data = request.get_json()
    return report.getbalancetrader(data)

@app.route('/getestelamstocksheet',methods = ['POST', 'GET'])
def getestelamstocksheet():
    data = request.get_json()
    return report.getestelamstocksheet(data)

@app.route('/checkstocksheet',methods = ['POST', 'GET'])
def checkstocksheet():
    data = request.get_json()
    return dara.getSheetpngAdmin(data)

@app.route('/getpersonalnobourse',methods = ['POST', 'GET'])
def getpersonalnobourse():
    data = request.get_json()
    return report.getpersonalnobourse(data)

@app.route('/deltransaction',methods = ['POST', 'GET'])
def deltransaction():
    data = request.get_json()
    return dataManagment.deltransaction(data)

@app.route('/setnewbankbalance',methods = ['POST', 'GET'])
def setnewbankbalance():
    data = request.get_json()
    return dataManagment.setnewbankbalance(data)

@app.route('/getassetfund',methods = ['POST', 'GET'])
def getassetfund():
    data = request.get_json()
    return report.getassetfund(data)

@app.route('/getrankfixin',methods = ['POST', 'GET'])
def getrankfixin():
    data = request.get_json()
    return report.getrankfixin(data)

@app.route('/settransaction',methods = ['POST', 'GET'])
def setTransaction():
    data = request.get_json()
    return dataManagment.setTransaction(data)

@app.route('/getpersonaldata',methods = ['POST', 'GET'])
def getpersonaldata():
    data = request.get_json()
    return report.getpersonaldata(data)

@app.route('/personalinassembly',methods = ['POST', 'GET'])
def personalinassembly():
    data = request.get_json()
    return report.personalinassembly(data)

@app.route('/addpersonalassembly',methods = ['POST', 'GET'])
def addpersonalassembly():
    data = request.get_json()
    return dataManagment.addpersonalassembly(data)

@app.route('/delbankassetfund',methods = ['POST', 'GET'])
def delbankassetfund():
    data = request.get_json()
    return dataManagment.delbankassetfund(data)


@app.route('/gettransactions',methods = ['POST', 'GET'])
def gettransactions():
    data = request.get_json()
    return report.gettransactions(data)

@app.route('/addtradernobourse',methods = ['POST', 'GET'])
def addtradernobourse():
    data = request.get_json()
    return dataManagment.addtradernobourse(data)

@app.route('/space', methods=['GET'])
def space():
    return dataManagment.space(request)

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

@app.route('/delpersonalassembly',methods = ['POST', 'GET'])
def delpersonalassembly():
    data = request.get_json()
    return dataManagment.delpersonalassembly(data)

@app.route('/getdatavotes',methods = ['POST', 'GET'])
def getdatavotes():
    data = request.get_json()
    return report.getdatavotes(data)

@app.route('/addvoteasemboly',methods = ['POST', 'GET'])
def addvoteasemboly():
    data = request.get_json()
    return report.addvoteasemboly(data)


@app.route('/getassembly',methods = ['POST', 'GET'])
def mgetassembly():
    data = request.get_json()
    return report.getassembly(data)

@app.route('/getsheetassembly',methods = ['POST', 'GET'])
def getsheetassembly():
    data = request.get_json()
    return report.getsheetassembly(data)

@app.route('/getresultvotes',methods = ['POST', 'GET'])
def getresultvotes():
    data = request.get_json()
    return report.getresultvotes(data)

@app.route('/addcapitalincrease',methods = ['POST', 'GET'])
def addcapitalincrease():
    data = request.get_json()
    return dataManagment.addcapitalincrease(data)

@app.route('/getcapitalincrease',methods = ['POST', 'GET'])
def getcapitalincrease():
    data = request.get_json()
    return report.getcapitalincrease(data)

@app.route('/delcapitalincrease',methods = ['POST', 'GET'])
def delcapitalincrease():
    data = request.get_json()
    return dataManagment.delcapitalincrease(data)

@app.route('/getpriority',methods = ['POST', 'GET'])
def getpriority():
    data = request.get_json()
    return report.getpriority(data)

@app.route('/endpriority',methods = ['POST', 'GET'])
def endpriority():
    data = request.get_json()
    return report.endpriority(data)

@app.route('/settransactionpriority',methods = ['POST', 'GET'])
def settransactionpriority():
    data = request.get_json()
    return dataManagment.settransactionpriority(data)

@app.route('/setpayprority',methods = ['POST', 'GET'])
def setpayprority():
    data = request.get_json()
    return dataManagment.setpayprority(data)

@app.route('/getprioritypay',methods = ['POST', 'GET'])
def getprioritypay():
    data = request.get_json()
    return dataManagment.getprioritypay(data)

@app.route('/delprioritypay',methods = ['POST', 'GET'])
def delprioritypay():
    data = request.get_json()
    return dataManagment.delprioritypay(data)

@app.route('/getprioritytransaction',methods = ['POST', 'GET'])
def getprioritytransaction():
    data = request.get_json()
    return report.getprioritytransaction(data)

@app.route('/delprioritytransaction',methods = ['POST', 'GET'])
def delprioritytransaction():
    data = request.get_json()
    return dataManagment.delprioritytransaction(data)

@app.route('/getresidual',methods = ['POST', 'GET'])
def getresidual():
    data = request.get_json()
    return report.getresidual(data)

@app.route('/preemptioncard',methods = ['POST', 'GET'])
def preemptioncard():
    data = request.get_json()
    return report.preemptioncard(data)

@app.route('/preemptioncardjpg',methods = ['POST', 'GET'])
def preemptioncardjpg():
    data = request.get_json()
    return report.preemptioncardjpg(data)

@app.route('/preemptioncardpdf',methods = ['POST', 'GET'])
def preemptioncardpdf():
    data = request.get_json()
    return report.preemptioncardpdf(data)

@app.route('/getxlsxpriority',methods = ['POST', 'GET'])
def getxlsxpriority():
    data = request.get_json()
    return report.getxlsxpriority(data)

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
    return dara.checkcookie(data)

@app.route('/dara/authenticationsession',methods = ['POST', 'GET'])
def dara_authenticationsession():
    data = request.get_json()
    return dara.authenticationsession(data)

@app.route('/dara/getcompany',methods = ['POST', 'GET'])
def dara_getcompany():
    data = request.get_json()
    return dara.getcompany(data)

@app.route('/dara/getSheetpng',methods = ['POST', 'GET'])
def dara_getSheetpng():
    data = request.get_json()
    return dara.getSheetpng(data)

@app.route('/dara/getassembly',methods = ['POST', 'GET'])
def getassembly():
    data = request.get_json()
    return dara.getassembly(data)

@app.route('/dara/static',methods = ['POST', 'GET'])
def darastatic():
    data = request.get_json()
    return dara.static(data)

@app.route('/dara/getsheet',methods = ['POST', 'GET'])
def dara_getsheet():
    data = request.get_json()
    return dara.getsheet(data)

@app.route('/desk/broker/volumetrade',methods = ['POST', 'GET'])
def desk_broker_volumeTrade():
    data = request.get_json()
    return report.desk_broker_volumeTrade(data)

@app.route('/desk/broker/dateavalibale',methods = ['POST', 'GET'])
def desk_broker_dateavalibale():
    data = request.get_json()
    return report.desk_broker_dateavalibale(data)

@app.route('/desk/broker/datenow',methods = ['POST', 'GET'])
def desk_broker_datenow():
    data = request.get_json()
    return report.datenow(data)

@app.route('/saveinvoce',methods = ['POST', 'GET'])
def saveinvoce():
    data = request.get_json()
    return report.saveinvoce(data)


@app.route('/desk/todo/addtask',methods = ['POST', 'GET'])
def desk_todo_addtask():
    data = request.get_json()
    return report.desk_todo_addtask(data)

@app.route('/desk/todo/getcontrol',methods = ['POST', 'GET'])
def desk_todo_getcontrol():
    data = request.get_json()
    return report.desk_todo_getcontrol(data)


@app.route('/desk/todo/gettask',methods = ['POST', 'GET'])
def desk_todo_gettask():
    data = request.get_json()
    return report.desk_todo_gettask(data)

@app.route('/desk/todo/setact',methods = ['POST', 'GET'])
def desk_todo_setact():
    data = request.get_json()
    return report.desk_todo_setact(data)

@app.route('/desk/todo/deltask',methods = ['POST', 'GET'])
def desk_todo_deltask():
    data = request.get_json()
    return report.desk_todo_deltask(data)

@app.route('/desk/broker/gettraders',methods = ['POST', 'GET'])
def desk_broker_gettraders():
    data = request.get_json()
    return report.desk_broker_gettraders(data)

@app.route('/desk/broker/turnover',methods = ['POST', 'GET'])
def desk_broker_turnover():
    data = request.get_json()
    return report.desk_broker_turnover(data)

@app.route('/getratretasst',methods = ['POST', 'GET'])
def getratretasst():
    data = request.get_json()
    return report.getratretasst(data)

@app.route('/desk/getinfocode',methods = ['POST', 'GET'])
def desk_getinfocode():
    data = request.get_json()
    return report.getinfocode(data)

@app.route('/desk/sabad/addcodetrader',methods = ['POST', 'GET'])
def desk_sabad_addcodetrader():
    data = request.get_json()
    return report.desk_sabad_addcodetrader(data)

@app.route('/dashpotantial',methods = ['POST', 'GET'])
def dashpotantial():
    data = request.get_json()
    return report.dashpotantial(data)

@app.route('/calculator/fixincom',methods = ['POST', 'GET'])
def calculator_fixincom():
    data = request.get_json()
    return report.calculator_fixincom(data)

@app.route('/desk/sabad/codetrader',methods = ['POST', 'GET'])
def desk_sabad_codetrader():
    data = request.get_json()
    return report.codetrader(data)

@app.route('/getcomparetop',methods = ['POST', 'GET'])
def getcomparetop():
    data = request.get_json()
    return report.getcomparetop(data)

@app.route('/desk/sabad/delcodetrade',methods = ['POST', 'GET'])
def desk_sabad_delcodetrade():
    data = request.get_json()
    return report.delcodetrade(data)

@app.route('/desk/sabad/turnoverpercode',methods = ['POST', 'GET'])
def desk_sabad_turnoverpercode():
    data = request.get_json()
    return report.turnoverpercode(data)

@app.route('/customerremain',methods = ['POST', 'GET'])
def CustomerRemain():
    data = request.get_json()
    return report.CustomerRemain(data)

@app.route('/valuefundinser',methods = ['POST', 'GET'])
def valuefundinseris():
    data = request.get_json()
    return report.valuefundinseris(data)

@app.route('/getassetmixbank',methods = ['POST', 'GET'])
def getassetmixbank():
    data = request.get_json()
    return report.getassetmixbank(data)

@app.route('/customerphonebook',methods = ['POST', 'GET'])
def customerphonebook():
    data = request.get_json()
    return report.customerphonebook(data)

@app.route('/getassetmixoraqdol',methods = ['POST', 'GET'])
def getassetmixoraqdol():
    data = request.get_json()
    return report.getassetmixoraqdol(data)

@app.route('/getassetmixoraqnon',methods = ['POST', 'GET'])
def getassetmixoraqnon():
    data = request.get_json()
    return report.getassetmixoraqnon(data)

@app.route('/getassetmixoraqsah',methods = ['POST', 'GET'])
def getassetmixoraqsah():
    data = request.get_json()
    return report.getassetmixoraqsah(data)

@app.route('/getaccbank',methods = ['POST', 'GET'])
def getaccbank():
    data = request.get_json()
    return report.getaccbank(data)



@app.route('/setholliday',methods = ['POST', 'GET'])
def setholliday():
    data = request.get_json()
    return report.setholliday(data)

@app.route('/getholliday',methods = ['POST', 'GET'])
def getholliday():
    data = request.get_json()
    return report.getholliday(data)


# اطلاعات کاربر چک کنه

@app.route ('/service/datacustomer' , methods = ['POST'])
def service_data_customer () :
    data = request.get_json()
    return report.service_data_customer (data)



# دارایی کاربر در کارگزاری و وجود کاربر در کارگزاری
@app.route ('/service/assetcustomer' , methods = ['POST'])
def service_data_assetcustomer () :
    data = request.get_json()
    return report.service_data_assetcustomer (data)


@app.route('/marketing/cityregisternobours', methods = ['POST'])
def city_nobourse () :
    data = request.get_json()
    return marketing.city_nobourse (data)


@app.route('/marketing/symbolregisternobours', methods = ['POST'])
def symbol_nobours () :
    data = request.get_json()
    return marketing.symbol_nobours (data)



@app.route('/marketing/fillter', methods = ['POST'])
def fillter () :
    data = request.get_json()
    return marketing.fillter (data)


@app.route('/marketing/bank', methods = ['POST'])
def bank_broker () :
    data = request.get_json()
    return marketing.bank_broker (data)


@app.route('/marketing/brokerbranch', methods = ['POST'])
def broker_branch () :
    data = request.get_json()
    return marketing.broker_branch (data)



@app.route('/marketing/marketinglist', methods = ['POST'])
def marketing_list () :
    data = request.get_json()
    return marketing.marketing_list (data)




@app.route('/marketing/columnmarketing', methods = ['POST'])
def column_marketing () :
    data = request.get_json()
    return marketing.column_marketing (data)



@app.route('/marketing/editfillter', methods = ['POST'])
def edit_config () :
    data = request.get_json()
    return marketing.edit_config (data)



@app.route('/marketing/deleteconfig', methods = ['POST'])
def delete_config () :
    data = request.get_json()
    return marketing.delete_config (data)



@app.route('/marketing/perviewcontext', methods = ['POST'])
def perviewcontext () :
    data = request.get_json()
    return marketing.perViewContent (data)



@app.route('/marketing/editcontext', methods = ['POST'])
def edit_context () :
    data = request.get_json()
    return marketing.edit_context (data)



@app.route('/marketing/viewconfig', methods = ['POST'])
def ViewConfig () :
    data = request.get_json()
    return marketing.ViewConfig (data)


@app.route('/marketing/sendsms', methods = ['POST'])
def send_message () :
    data = request.get_json()
    return marketing.send_message (data)


@app.route('/marketing/insurance_item', methods = ['POST'])
def insurance_item () :
    data = request.get_json()
    return marketing.insurance_item (data)



@app.route('/marketing/Insurance_field', methods = ['POST'])
def Insurance_field () :
    data = request.get_json()
    return marketing.Insurance_field (data)


@app.route('/marketing/insurance_companies', methods = ['POST'])
def insurance_companies () :
    data = request.get_json()
    return marketing.insurance_companies (data)



@app.route('/marketing/insurance_consultant', methods = ['POST'])
def insurance_consultant () :
    data = request.get_json()
    return marketing.insurance_consultant (data)





@app.route('/marketing/set_status', methods = ['POST'])
def set_status () :
    data = request.get_json()
    return marketing.set_status (data)




@app.route('/marketing/bours_city', methods = ['POST'])
def bours_city () :
    data = request.get_json()
    return marketing.bours_city (data)


@app.route('/marketing/bours_branch', methods = ['POST'])
def bours_branch () :
    data = request.get_json()
    return marketing.bours_branch (data)



@app.route('/marketing/bours_asset', methods = ['POST'])
def bours_asset () :
    data = request.get_json()
    return marketing.bours_asset (data)



@app.route('/fixincom/compareprice',methods = ['POST', 'GET'])
def fixincom_compareprice():
    data = request.get_json()
    return report.fixincom_compareprice(data)

@app.route('/setting/usermanage',methods = ['POST'])
def setting_usermanage():
    data = request.get_json()
    return report.setting_usermanage(data)

@app.route('/dara/balancestock',methods = ['POST'])
def dara_balance_stock():
    data = request.get_json()
    return dara.balance_stock(data)

@app.route('/report/balancestock',methods = ['POST'])
def report_balance_stock():
    data = request.get_json()
    return report.balance_stock(data)

if __name__ == '__main__':
    serve(app, host="0.0.0.0", port=8080,threads= 8)
    #app.run(host='0.0.0.0',port=8000, debug=True)


