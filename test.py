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

import selenium

print(selenium.__version__)

# file_path = os.path.abspath(__file__)

# download_path = os.path.join(os.path.dirname(file_path), 'download')

# prefs = {
#     "download.default_directory": download_path,
#     "download.prompt_for_download": False,
#     "download.directory_upgrade": True,
#     "safebrowsing.enabled": True
# }

# options = webdriver.EdgeOptions()


# options.add_experimental_option('prefs', prefs)

# driver = webdriver.Edge(executable_path='msedgedriver.exe',options=options)



# url = 'https://tsetmc.com/'
# driver.get(url)
# time.sleep(3)
# driver.find_element(by=By.XPATH, value='/html/body/div/div/header/div/div/div[2]/a[5]').click()
# time.sleep(3)
# driver.find_element(by=By.XPATH, value='/html/body/div[2]/div[3]/div/input').send_keys('ویسا')
# time.sleep(3)
# element = driver.find_element(by=By.LINK_TEXT, value='ويسا - سرمايه گذاري ايساتيس پويا')
# href = element.get_attribute('href')
# driver.get(href)
# time.sleep(3)
# driver.find_element(by=By.XPATH, value='/html/body/div/div/div[2]/div[2]/ul/li[3]/a').click()
# time.sleep(3)
# driver.find_element(by=By.XPATH, value='/html/body/div/div/div[2]/div[3]/div[1]/div[2]/div[4]').click()
# time.sleep(5)