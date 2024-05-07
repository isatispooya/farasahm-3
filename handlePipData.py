import functionPipData
from Fnc import CulcTime
from time import sleep
import datetime
import multiprocessing


def CirOne():
    while True:
        now = datetime.datetime.now()
        if CulcTime(1, now):
            functionPipData.GetAllTradeToDay()
            functionPipData.get_asset_customer()
            sleep(60*10)
        else:
            sleep(60)

def CirTwo():
    while True:
        now = datetime.datetime.now()
        if CulcTime(2, now):
            functionPipData.GetAllTradeLast30Day()
            functionPipData.TseRepir()
            sleep(60*60*24)
        else:
            sleep(60)


def CirTree():
    while True:
        now = datetime.datetime.now()
        if CulcTime(3, now):
            functionPipData.CuostomerRemain()
            sleep(60*60*24)
        else:
            sleep(60)

def CirFour():
    while True:
        now = datetime.datetime.now()
        if CulcTime(3, now):
            functionPipData.desk_broker_volumeTrade_cal()
            functionPipData.task_desk_broker_turnover_cal()
            functionPipData.get_asset_funds()
            functionPipData.getAssetCoustomerByFixincome()
            sleep(60*60*24)
        else:
            sleep(60)




if __name__ == "__main__":


    processes = [
        multiprocessing.Process(target=CirOne),
        multiprocessing.Process(target=CirTwo),
        multiprocessing.Process(target=CirTree),
        multiprocessing.Process(target=CirFour),
    ]

    # شروع هر پردازه
    for p in processes:
        p.daemon = True
        p.start()

    # انتظار برای پایان هر پردازه
    for p in processes:
        p.join()