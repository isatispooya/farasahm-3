import requests
import pandas as pd
import pymongo
from selenium import webdriver
import warnings
import time
from selenium.webdriver.common.by import By
import os
import Fnc

warnings.filterwarnings("ignore")
client = pymongo.MongoClient()
farasahm_db = client['farasahm2']


def WC():
    etfList = pd.DataFrame([x for x in farasahm_db['fixIncome'].find({},{'_id':0,'نماد':1,'url':1})])
    options = webdriver.EdgeOptions()
    file_path = os.path.abspath(__file__)
    download_path = os.path.join(os.path.dirname(file_path), 'download')
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Edge(executable_path='msedgedriver.exe',options=options)

    for i in etfList.index:
        try:
            symbol = etfList['نماد'][i]
            print(symbol)
            collection = pd.DataFrame(farasahm_db['fixIncomeHistori'].find({'name':symbol}))
            lastUpdateRecord = collection[collection['dateInt']==collection['dateInt'].max()].to_dict('records')[0]
            lastUpdate = lastUpdateRecord['dateInt']
            driver.get(etfList['url'][i])
            lop = True
            while lop:
                try:
                    time.sleep(1)
                    nav = driver.find_element(by=By.XPATH,value='/html/body/div/div/div[2]/div[3]/div[3]/div[1]/div[2]/div[6]/table/tbody/tr[1]/td[2]').text
                    nav = int(nav.replace(',',''))
                    lop =False
                except:print(f'{symbol} try nav')

            driver.find_element(by=By.CSS_SELECTOR,value='#root > div > div:nth-child(3) > div.menuHolder2 > ul > li:nth-child(3) > a').click()
            lop = True
            while lop:
                try:
                    time.sleep(1)
                    driver.find_element(by=By.XPATH,value='/html/body/div/div/div[2]/div[3]/div[1]/div[2]/div[4]').click()
                    dir = os.listdir(download_path)
                    time.sleep(3)
                    dir = os.listdir(download_path)
                    df = pd.read_csv('download/'+dir[0])
                    for file_name in dir:
                        file_path = os.path.join(download_path, file_name)
                        os.remove(file_path)
                    lop = False
                except:print(f'{symbol} histori')

            df.columns = ['<TICKER>', 'date', 'first_price', 'highest_price', 'lowest_price', 'close_price','trade_value', 'trade_volume', 'trade_number', '<PER>', 'first_price', 'final_price']
            df = df[['highest_price','lowest_price','final_price','close_price','first_price','trade_value','trade_volume','trade_number','date']]
            df['dateInt'] = [Fnc.gorgianIntToJalaliInt(x) for x in df['date']]
            df['date'] = df['dateInt']
            df = df[df['dateInt']>int(lastUpdate)]
            if len(df)>0:
                df['nav'] = 0
                df['name'] = symbol
                df = df.to_dict('records')
                df[0]['nav'] = nav
                farasahm_db['fixIncomeHistori'].insert_many(df)
        except:
            pass


    driver.quit()




#schedule.every().day.at("19:30").do(WC)
#
#while True:
#    schedule.run_pending()
#    time.sleep(60)
