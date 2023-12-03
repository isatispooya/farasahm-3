import requests
import pandas as pd
import pymongo
from selenium import webdriver
import warnings
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import os
import Fnc
warnings.filterwarnings("ignore")

client = pymongo.MongoClient()
farasahm_db = client['farasahm2']

class TseCrawling:
    def __init__(self):
        file_path = os.path.abspath(__file__)
        self.options = webdriver.EdgeOptions()
        self.download_path = os.path.join(os.path.dirname(file_path), 'download')
        prefs = {
            "download.default_directory": self.download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        self.options.add_experimental_option('prefs', prefs)
        self.driver = webdriver.Edge(executable_path='msedgedriver.exe',options=self.options)

    def fund_list(self):
        df = requests.get('http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx?d=0').content
        df = pd.read_excel(df,header=2)
        df = df[['نماد','نام']]
        df['basic'] = df['نماد'].apply(Fnc.has_number)
        df = df[df['basic']!=True]
        df['basic'] = df['نام'].apply(Fnc.is_Fund)
        df = df[df['basic']==True]
        df['type'] = df['نام'].apply(Fnc.type_fund)
        df = df.drop(columns=['basic'])
        df = df.to_dict('records')
        return df


    def getHistoriPriceByFullName(self,fullName,symbol,type):
        self.driver.get('http://tsetmc.com/')
        wait = WebDriverWait(self.driver, 60)
        time.sleep(2)
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div/header/div/div/div[2]/a[5]")))
        time.sleep(1)
        self.driver.find_element(By.XPATH, "/html/body/div/div/header/div/div/div[2]/a[5]").click()
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[3]/div/input")))
        self.driver.find_element(By.XPATH, "/html/body/div[2]/div[3]/div/input").click()
        self.driver.find_element(By.XPATH, "/html/body/div[2]/div[3]/div/input").send_keys(fullName)
        self.driver.find_element(By.XPATH, "/html/body/div[2]/div[3]/div/input").send_keys(' ')
        wait.until(EC.presence_of_element_located((By.XPATH, f"//a[contains(text(), '{fullName}')]")))
        element = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{fullName}')]")
        link = element.get_attribute('href')
        self.driver.get(link)
        time.sleep(3)
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div/div[2]/div[3]/div[3]/div[1]/div[2]/div[6]/table/tbody/tr[1]/td[2]")))
        try:
            nav = self.driver.find_element(By.XPATH,"/html/body/div/div/div[2]/div[3]/div[3]/div[1]/div[2]/div[6]/table/tbody/tr[1]/td[2]").text
            nav = int(nav.replace(',',''))
        except:
            nav = 0
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div/div[2]/div[2]/ul/li[3]/a")))
        self.driver.find_element(By.XPATH, "/html/body/div/div/div[2]/div[2]/ul/li[3]/a").click()
        time.sleep(3)
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div/div[2]/div[3]/div[1]/div[2]/div[4]")))
        df = self.driver.find_element(By.XPATH, "/html/body/div/div/div[2]/div[3]/div[1]/div[2]/div[4]").click()
        time.sleep(5)
        dir = os.listdir(self.download_path)
        df = pd.read_csv('download/'+dir[0])
        for file_name in dir:
            file_path = os.path.join(self.download_path, file_name)
            os.remove(file_path)
        df.columns = ['<TICKER>', 'date', 'first_price', 'highest_price', 'lowest_price', 'close_price','trade_value', 'trade_volume', 'trade_number', '<PER>', 'first_price', 'final_price']
        df = df[['highest_price','lowest_price','final_price','close_price','first_price','trade_value','trade_volume','trade_number','date']]
        df['dateInt'] = [Fnc.gorgianIntToJalaliInt(x) for x in df['date']]
        df['date'] = df['dateInt']
        collection = pd.DataFrame(farasahm_db['sandoq'].find({'fullName':fullName}))

        if len(collection)>0:
            lastUpdateRecord = collection[collection['dateInt']==collection['dateInt'].max()].to_dict('records')[0]
            lastUpdate = lastUpdateRecord['dateInt']
            df = df[df['dateInt']>int(lastUpdate)]

        if len(df)>0:
            df['nav'] = 0
            df['fullName'] = fullName
            df['symbol'] = symbol
            df['type'] = type
            df = df.to_dict('records')
            df[0]['nav'] = nav
            farasahm_db['sandoq'].insert_many(df)
        return True
    
    @Fnc.retry_decorator(max_retries=3, sleep_duration=5)
    def get_all_fund(self):
        print('get_all_fund')
        list_fund = self.fund_list()

        self.getHistoriPriceByFullName("صندوق س خاتم ايساتيس پويا-ثابت","خاتم","type")
        for i in list_fund:
            count_try = 0
            while count_try < 5:
                try:
                    print(i['نماد'])
                    self.getHistoriPriceByFullName(i['نام'],i['نماد'],i['type'])
                    count_try = 10
                except:
                    count_try += 1
                    print(f'error {i}')
                    
    @Fnc.retry_decorator(max_retries=3, sleep_duration=5)
    def getOragh(self):
        print('get farabourse for oragh')
        update = Fnc.todayIntJalali()
        url_ifb = 'https://www.ifb.ir/ytm.aspx'
        self.driver.get(url_ifb)
        time.sleep(5)
        KhazanehGrid = pd.read_html(self.driver.find_element(By.CLASS_NAME,'KhazanehGrid').get_attribute('outerHTML'))[0]
        KhazanehGrid['type'] = 'بدون کوپن'
        mGrid = pd.read_html(self.driver.find_element(By.CLASS_NAME,'mGrid').get_attribute('outerHTML'))[0]
        mGrid['type'] = 'با کوپن'
        df = pd.concat([KhazanehGrid, mGrid])
        df = df.drop(columns=['ردیف'])
        df['YTM'] = [float(str(x).replace('/','.').replace('%','')) for x in df['YTM']]
        df['بازده ساده'] = [float(str(x).replace('/','.').replace('%','')) for x in df['بازده ساده']]
        df['update'] = update
        df['market'] = 'فرابورس'
        df = df.to_dict('records')
        farasahm_db['oraghYTM'].delete_many({'update':update,'market':'فرابورس'})
        farasahm_db['oraghYTM'].insert_many(df)


    @Fnc.retry_decorator(max_retries=3, sleep_duration=5)
    def getAmariNav(self):
        print('get Nav Amari')
        lst = [
            {'symbol':'خاتم','url':'http://etf.isatispm.com/','elementAmari':'/html/body/div[4]/div[3]/div[4]/div/span[2]','emelentDate':'/html/body/div[4]/div[3]/div[1]/div/span[2]','countunit':'/html/body/div[4]/div[3]/div[6]/div/span[2]'},
        ]
        for i in lst:
            self.driver.get(i['url'])
            time.sleep(5)
            amari = self.driver.find_element(By.XPATH,i['elementAmari']).text
            dateAmary = self.driver.find_element(By.XPATH,i['emelentDate']).text
            countunit = self.driver.find_element(By.XPATH,i['countunit']).text
            amari = amari.replace('ریال','')
            dateAmary = dateAmary.replace('/','')
            countunit = int(countunit.replace(',',''))
            amari = int(amari.replace(',',''))
            dateAmary = int(dateAmary.replace('/',''))
            farasahm_db['sandoq'].update_many({'symbol':i['symbol'],'dateInt':dateAmary},{'$set':{'navAmary':amari,'countunit':countunit}})

    @Fnc.retry_decorator(max_retries=3, sleep_duration=5)
    def getOraghBoursi(self):
        print('get oragh bursi')
        url = 'https://old.tse.ir/MarketWatch-ang.html?cat=debt'
        self.driver.get(url)
        time.sleep(5)
        self.driver.find_element(By.XPATH, '/html/body/div/div[9]/div[1]/div[2]/ul[3]/li[4]/a').click()
        time.sleep(5)
        dir = os.listdir(self.download_path)
        df = pd.read_excel('download/'+dir[0],header=2)
        for file_name in dir:
            file_path = os.path.join(self.download_path, file_name)
            os.remove(file_path)
        print(df.columns)
        df = df[['Unnamed: 0','مقدار.2','تاریخ','YTM(%).1']]
        df = df.rename(columns={'Unnamed: 0':'نماد','مقدار.2':'قیمت معامله شده هر ورقه','تاریخ':'تاریخ آخرین روز معاملاتی','YTM(%).1':'YTM'})
        df['YTM'] = df['YTM'].apply(float)
        df = df[df['df']>0]
        df['type'] = 'بدون کوپن'
        df['market'] = 'بورس'
        update = Fnc.todayIntJalali()
        df['update'] = Fnc.todayIntJalali()
        farasahm_db['oraghYTM'].delete_many({'update':update,'market':'بورس'})
        farasahm_db['oraghYTM'].insert_many(df.to_dict('records'))


    