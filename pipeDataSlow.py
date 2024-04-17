import multiprocessing
import ApiMethods
from dataManagment import desk_broker_turnover_cal, desk_broker_volumeTrade_cal
import Fnc

def task_GetAllTradeLastDate():
    while True:
        ApiMethods.GetAllTradeLastDate()

def task_TseRepir():
    while True:
        Fnc.TseRepir()

def task_handle_CuostomerRemain():
    while True:
        ApiMethods.handle_CuostomerRemain()

def task_desk_broker_volumeTrade_cal():
    while True:
        desk_broker_volumeTrade_cal()

def task_desk_broker_turnover_cal():
    while True:
        desk_broker_turnover_cal()

if __name__ == "__main__":
    # ایجاد پردازه‌ها برای هر تابع
    processes = [
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
