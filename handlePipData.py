import functionPipData
from Fnc import CulcTime
from time import sleep
import datetime
import threading
from setting import farasahmDb




sleep_no_time = 60

def CirOne():
    func = 1
    while True:
        now = datetime.datetime.now()
        try:
            if CulcTime(1, now):
                functionPipData.getTseToDay()
                functionPipData.GetAllTradeToDay()
                functionPipData.get_asset_customer()
                sleep(60*10)
            else:
                sleep(sleep_no_time)
        except:
            farasahmDb['log'].insert_one({'func':func, 'act':'except', 'date':now})
            sleep(60)

def CirTwo():
    func = 2
    while True:
        now = datetime.datetime.now()
        try:
            if CulcTime(0, now):
                functionPipData.GetAllTradeLast30Day()
                functionPipData.TseRepir()
                sleep(60*60*12)
            else:
                sleep(sleep_no_time)
        except:
            farasahmDb['log'].insert_one({'func':func, 'act':'except', 'date':now})
            sleep(5)

def CirTree():
    func = 3
    
    while True:
        now = datetime.datetime.now()
        try:
            if CulcTime(3, now):
                functionPipData.CuostomerRemain()
                sleep(60*60*12)
            else:
                sleep(sleep_no_time)
        except:
            farasahmDb['log'].insert_one({'func':func, 'act':'except', 'date':now})
            sleep(5)
            
def CirFour():
    func = 4
    while True:
        now = datetime.datetime.now()
        try:
            if CulcTime(3, now):
                functionPipData.desk_broker_volumeTrade_cal()
                functionPipData.task_desk_broker_turnover_cal()
                sleep(60*60*12)
            else:
                sleep(sleep_no_time)
        except:
            farasahmDb['log'].insert_one({'func':func, 'act':'except', 'date':now})
            sleep(5)

def CirFive():
    func = 5
    while True:
        now = datetime.datetime.now()
        try:
            if CulcTime(2, now):
                functionPipData.get_asset_funds()
                functionPipData.getAssetCoustomerByFixincome()
                sleep(60*60*12)
            else:
                sleep(sleep_no_time)
        except:
            farasahmDb['log'].insert_one({'func':func, 'act':'except', 'date':now})
            sleep(5)

# def CirSix():
#     func = 6
#     Tse = TseCrawling()
#     while True:
#         now = datetime.datetime.now()
#         try:
#             if CulcTime(3, now):
#                 Tse.getOragh()
#                 Tse.getOraghBoursi()
#                 Tse.getAmariNav()
#                 sleep(60*60*12)
#             else:
#                 farasahmDb['log'].insert_one({'func':func, 'act':'sleep', 'date':now})
#                 sleep(sleep_no_time)
#         except:
#             farasahmDb['log'].insert_one({'func':func, 'act':'except', 'date':now})
#             sleep(5)
        
# def CirSeven():
#     func = 7
#     Tse = TseCrawling()
#     while True:
#         now = datetime.datetime.now()
#         try:
#             if CulcTime(2, now):
#                 Tse.get_all_fund()
#                 sleep(60*60*12)
#             else:
#                 farasahmDb['log'].insert_one({'func':func, 'act':'sleep', 'date':now})
#                 sleep(sleep_no_time)
#         except:
#             farasahmDb['log'].insert_one({'func':func, 'act':'except', 'date':now})
#             sleep(5)





# ایجاد تردها برای اجرای توابع به صورت موازی
thread1 = threading.Thread(target=CirOne)
thread2 = threading.Thread(target=CirTwo)
thread3 = threading.Thread(target=CirTree)
thread4 = threading.Thread(target=CirFour)
thread5 = threading.Thread(target=CirFive)
# thread6 = threading.Thread(target=CirSix)
# thread7 = threading.Thread(target=CirSeven)

# شروع اجرای توابع
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
# thread6.start()
# thread7.start()

# گذاشتن تردها در حالت join
thread1.join()
thread2.join()
thread3.join()
thread4.join()
thread5.join()         
# thread6.join()
# thread7.join()
