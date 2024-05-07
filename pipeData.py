import Fnc
import ApiMethods
import multiprocessing
from dataManagment import desk_broker_turnover_cal, desk_broker_volumeTrade_cal
import time


def task_GetAllTradeLastDate():
    while True:
        if Fnc.CulcTime(2):
            ApiMethods.GetAllTradeLastDate()
        else:
            time.sleep(60*10)

def task_TseRepir():
    while True:
        if Fnc.CulcTime(3):
            Fnc.TseRepir()
        else:
            time.sleep(60*15)

def task_handle_CuostomerRemain():
    while True:
        if Fnc.CulcTime(3):
            ApiMethods.handle_CuostomerRemain()
        else:
            time.sleep(60*15)

def task_desk_broker_volumeTrade_cal():
    while True:
        if Fnc.CulcTime(3):
            desk_broker_volumeTrade_cal()
        else:
            time.sleep(60*15)

def task_desk_broker_turnover_cal():
    while True:
        if Fnc.CulcTime(3):
            desk_broker_turnover_cal()
        else:
            time.sleep(60*15)


def task_GetAllTradeInDate():
    while True:
        if Fnc.CulcTime(1):
            toDay = Fnc.toDayJalaliListYMD()
            toDayIntJal = Fnc.todayIntJalali()
            ApiMethods.GetAllTradeInDate(toDay,toDayIntJal)
        else:
            time.sleep(60*5)


def task_get_asset_funds():
    while True:
        if Fnc.CulcTime(2):
            ApiMethods.get_asset_funds()
        else:
            time.sleep(60*10)



def task_getAssetCoustomerByFixincome():
    while True:
        if Fnc.CulcTime(2):
            ApiMethods.getAssetCoustomerByFixincome()
        else:
            time.sleep(60*10)

if __name__ == "__main__":
    # ایجاد پردازه‌ها برای هر تابع
    processes = [
        multiprocessing.Process(target=task_GetAllTradeInDate),
        multiprocessing.Process(target=task_get_asset_funds),
        multiprocessing.Process(target=task_getAssetCoustomerByFixincome),
        multiprocessing.Process(target=task_GetAllTradeLastDate),
        multiprocessing.Process(target=task_TseRepir),
        multiprocessing.Process(target=task_handle_CuostomerRemain),
        multiprocessing.Process(target=task_desk_broker_volumeTrade_cal),
        multiprocessing.Process(target=task_desk_broker_turnover_cal)
    ]

    # شروع هر پردازه
    for p in processes:
        p.start()

    # انتظار برای پایان هر پردازه
    for p in processes:
        p.join()
