import requests
import pandas as pd
import xml.etree.ElementTree as ET
import function
import xml.etree.ElementTree as ET
url = "http://tbs.ipb.ir/TadbirPardaz.CustomerClubExternalService"

def GetBranchList():
    payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n      <tem:GetBranchList/>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetBranchList',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    Body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetBranchListResponse']['{http://tempuri.org/}GetBranchListResult']['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchDto']
    lst = []
    for i in Body:
        dic = {}
        dic['BranchDestinationType'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchDestinationType']['text']
        dic['BranchID'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchID']['text']
        dic['BranchName'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchName']['text']
        dic['BranchSymbol'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchSymbol']['text']
        dic['BranchTraderCode'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchTraderCode']['text']
        dic['OfficeId'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}OfficeId']['{http://www.w3.org/2001/XMLSchema-instance}nil']
        lst.append(dic)
    return lst



def GetTradeTimeTrades(fromTime, toTime,page):
    payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n   <tem:GetTradeTimeTrades>\n    <tem:fromTime>{fromTime}</tem:fromTime>\n  <tem:toTime>{toTime}</tem:toTime>\n  <tem:page>{page}</tem:page>\n  </tem:GetTradeTimeTrades>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetTradeTimeTrades',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetTradeTimeTradesResponse']['{http://tempuri.org/}GetTradeTimeTradesResult']['{http://schemas.datacontract.org/2004/07/TadbirPardaz.Infrastructure.DataAccess}Result']['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeDto']
    lst = []
    for i in body:
        dic = {}
        dic['AddedValueTax'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}AddedValueTax']['text']
        dic['BondDividend'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BondDividend']['text']
        dic['BranchID'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchID']['text']
        dic['BranchTitle'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}BranchTitle']['text']
        dic['Discount'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}Discount']['text']
        dic['InstrumentCategory'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}InstrumentCategory']['text']
        dic['MarketInstrumentISIN'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}MarketInstrumentISIN']['text']
        dic['NetPrice'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}NetPrice']['text']
        dic['Price'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}Price']['text']
        dic['TotalCommission'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TotalCommission']['text']
        dic['TradeCode'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeCode']['text']
        dic['TradeDate'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeDate']['text']
        dic['TradeItemBroker'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeItemBroker']['text']
        dic['TradeItemRayanBourse'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeItemRayanBourse']['text']
        dic['TradeNumber'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeNumber']['text']
        dic['TradeStationType'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeStationType']['text']
        dic['TradeSymbol'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeSymbol']['text']
        dic['TradeType'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeType']['text']
        dic['TransferTax'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TransferTax']['text']
        dic['Volume'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}Volume']['text']
        lst.append(dic)
    return lst



def GetCustomerPortfolio(nationalIdentification):
    payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n   <tem:GetCustomerPortfolio>\n    <tem:nationalIdentification>{nationalIdentification}</tem:nationalIdentification>\n   </tem:GetCustomerPortfolio>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetCustomerPortfolio',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetCustomerPortfolioResponse']['{http://tempuri.org/}GetCustomerPortfolioResult']['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}PortfolioDto']
    lst = []
    for i in body:
        dic = {}
        dic['ISIN'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}ISIN']['text']
        dic['Symbol'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}Symbol']['text']
        dic['Volume'] = i['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}Volume']['text']
        lst.append(dic)
    return lst



def GetCustomerByNationalCode(nationalIdentification):
    payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n   <tem:GetCustomerByNationalCode>\n    <tem:nationalIdentification>{nationalIdentification}</tem:nationalIdentification>\n   </tem:GetCustomerByNationalCode>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetCustomerByNationalCode',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetCustomerByNationalCodeResponse']['{http://tempuri.org/}GetCustomerByNationalCodeResult']
    dic = {}
    for i in body:
        key = i.split('}')[1]
        value = body[i]
        if key == 'BankBranchName':
            dic[key] = value['{http://www.w3.org/2001/XMLSchema-instance}nil']
        else:
            if len(value)>0:
                dic[key] = value['text']
            else:
                dic[key] = 0
    return dic



def GetFirmByNationalIdentification(nationalIdentification):
    payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n   <tem:GetFirmByNationalIdentification>\n    <tem:nationalIdentification>{nationalIdentification}</tem:nationalIdentification>\n   </tem:GetFirmByNationalIdentification>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetFirmByNationalIdentification',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetFirmByNationalIdentificationResponse']['{http://tempuri.org/}GetFirmByNationalIdentificationResult']
    dic = {}
    for i in body:
        key = i.split('}')[1]
        value = body[i]
        if len(value)>0:
            dic[key] = value['text']
        else:
            dic[key] = 0
    return dic

def GetTradeList(Firm,nationalIdentification,yr,mn,dy):
    if Firm == True:
        tradeCode = GetFirmByNationalIdentification(nationalIdentification)['PAMCode']
    else:
        tradeCode = GetCustomerByNationalCode(nationalIdentification)['PAMCode']
    toDate = function.GenerateDate(yr,mn,dy)
    fromDate = function.nDayLastDate(toDate,7)
    payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n   <tem:GetTradeList>\n    <tem:tradeCode>{tradeCode}</tem:tradeCode>\n    <tem:fromDate>{fromDate}</tem:fromDate>\n    <tem:toDate>{toDate}</tem:toDate>\n   </tem:GetTradeList>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetTradeList',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetTradeListResponse']['{http://tempuri.org/}GetTradeListResult']['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeDto']
    lst = []
    for i in body:
        dic = {}
        for j in body[i].keys():
            key = i.split('}')[1]
            value = body[i]
            if len(value)>0:
                dic[key] = value['text']
            else:
                dic[key] = 0
        lst.append(dic)
    return lst


def GetCustomerRemain(nationalIdentification, market):
    '''
    market => TSE = 1, IME = 2, IEE = 3, EFP = 4 
    '''
    payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n   <tem:GetCustomerRemain>\n    <tem:nationalIdentification>{nationalIdentification}</tem:nationalIdentification>\n    <tem:market>{market}</tem:market>\n   </tem:GetCustomerRemain>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetCustomerRemain',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetCustomerRemainResponse']['{http://tempuri.org/}GetCustomerRemainResult']
    dic = {}
    for i in body.keys():
        key = i.split('}')[1]
        value = body[i]
        if key == 'CreditDate':
            dic['CreditDate'] = value['{http://www.w3.org/2001/XMLSchema-instance}nil']
        else:
            if len(value)>0:
                dic[key] = value['text']
            else:
                dic[key] = 0
    return dic

def GetDailyTradeList(yr,mn,dy,page,pageSize):
    tradeDate = function.GenerateDate(yr,mn,dy)
    payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\">\n   <soapenv:Header>\n <CustomHeaderMessage xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"TadbirPardaz.TBS/PrincipalHeader\">\n  <AuthenticationType xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <Delegate xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IpAddress xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <IsAuthenticated xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">false</IsAuthenticated>\n  <Name xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomHeaderMessage</Name>\n  <NameSpace xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">TadbirPardaz.TBS/PrincipalHeader</NameSpace>\n  <Password xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">123456aA$</Password>\n  <Roles xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\" xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserId xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\" i:nil=\"true\"/>\n  <UserName xmlns=\"http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.ServiceHost.Domain\">CustomerClub</UserName>\n </CustomHeaderMessage>\n   </soapenv:Header>\n   <soapenv:Body>\n   <tem:GetDailyTradeList>\n    <tem:tradeDate>{tradeDate}</tem:tradeDate>\n    <tem:page>{page}</tem:page>\n   <tem:pageSize>{pageSize}</tem:pageSize>\n   </tem:GetDailyTradeList>\n   </soapenv:Body>\n</soapenv:Envelope>"
    headers = {
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': 'http://tempuri.org/ICustomerClubExternalService/GetDailyTradeList',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.content
    response = response.decode("utf-8")
    root = ET.fromstring(response)
    xml_dict = function.element_to_dict(root)
    body = xml_dict['{http://schemas.xmlsoap.org/soap/envelope/}Body']['{http://tempuri.org/}GetDailyTradeListResponse']['{http://tempuri.org/}GetDailyTradeListResult']['{http://schemas.datacontract.org/2004/07/TadbirPardaz.Infrastructure.DataAccess}Result']['{http://schemas.datacontract.org/2004/07/TadbirPardaz.TBS.Domain.Entities.ExternalService}TradeDto']
    lst = []
    for i in body:
        dic = {} 
        for j in i.keys():
            key = j.split('}')[1]
            value = i[j]
            if len(value)>0:
                dic[key] = value['text']
            else:
                dic[key] = 0
        lst.append(dic)
    return lst
