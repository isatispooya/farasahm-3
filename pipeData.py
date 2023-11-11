import Fnc
import time
import ApiMethods
from dataManagment import desk_broker_volumeTrade_cal, desk_broker_turnover_cal
from crawlingTse import TseCrawling

Tse = TseCrawling()

while True:
    
    if Fnc.is_time_between(8,16) and Fnc.is_time_divisible(5):
        Fnc.getTseDate()
        Tse.getOragh()
        ApiMethods.GetAllTradeInDate()
        desk_broker_volumeTrade_cal()
        desk_broker_turnover_cal()
        ApiMethods.get_asset_funds()

    if Fnc.is_time_between(16,18):
        Fnc.getTse30LastDay()
        ApiMethods.GetAllTradeLastDate()

    if Fnc.is_time_between(18,20):
        Tse.get_all_fund()

