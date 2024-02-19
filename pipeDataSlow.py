
import ApiMethods
from dataManagment import desk_broker_turnover_cal, desk_broker_volumeTrade_cal
import Fnc
while True:
    Fnc.TseRepir()
    ApiMethods.handle_CuostomerRemain()
    desk_broker_volumeTrade_cal()
    desk_broker_turnover_cal()


