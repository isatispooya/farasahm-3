import Fnc
import WebCrawling
import time
import ApiMethods
from dataManagment import desk_broker_volumeTrade_cal, desk_broker_turnover_cal

while True:
    # این بخش حلقه برای خزش وب است
    if Fnc.is_time_between(20,22):
        webCrawlingCheak = False
        while webCrawlingCheak == False and Fnc.is_time_between(20,22):
            try:
                WebCrawling.WC()
                webCrawlingCheak = True
                print('Web Crawling successful')
            except:
                print('Web Crawling Break')
                time.sleep(60)
    try:
        if Fnc.is_time_between(8,17) and Fnc.is_time_divisible(15):
        # این بخش حلقه برای دریافت اطلاعات از کار گزاری است
            try:
                Fnc.getTseDate()
            except:
                print('crash getTseDate')
        # این بخش حلقه برای دریافت اطلاعات از کار گزاری است
            ApiMethods.GetAllTradeInDate()
            print('ended GetAllTradeInDate')
            desk_broker_volumeTrade_cal()
            desk_broker_turnover_cal()
        
        if Fnc.is_time_between(17,23):
            Fnc.getTse30LastDay()
            ApiMethods.GetAllTradeLastDate()

        time.sleep(60)
        print('End of circle')
    except:
        time.sleep(60)
        print('crash')
    



    