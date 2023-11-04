import Fnc
import time
import ApiMethods
from dataManagment import desk_broker_volumeTrade_cal, desk_broker_turnover_cal
from crawlingTse import TseCrawling

while True:
    if Fnc.is_time_between(8,17) and Fnc.is_time_divisible(5):
        # این بخش حلقه برای دریافت اطلاعات از کار گزاری است
        Fnc.getTseDate()
        # این بخش حلقه برای دریافت اطلاعات از کار گزاری است
        ApiMethods.GetAllTradeInDate()
        print('ended GetAllTradeInDate')
        desk_broker_volumeTrade_cal()
        desk_broker_turnover_cal()

    if Fnc.is_time_between(17,23):
        Fnc.getTse30LastDay()
        ApiMethods.GetAllTradeLastDate()

    # این بخش اطلاعات صندوق ها را از tse میگیرد و در دیتابیس میریزد
    if Fnc.is_time_between(18,21):
        Tse = TseCrawling()
        Tse.get_all_fund()

    print('End of circle')
    time.sleep(60)

        



    