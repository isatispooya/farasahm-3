from setting import farasahmDb
import json
from datetime import datetime
from marketing import fillter_registernobours, replace_placeholders , fillter_insurance
import pandas as pd
from Login import SendSms
from time import sleep


def check_config () :
    active_configs = list(farasahmDb['marketing_config'].find({"status": True, "context": {"$ne": ""}}))
    now = datetime.now()
    for config in active_configs:
        send_time = config.get('send_time')
        if send_time:
            send_time = int(send_time)
            send_time = send_time/1000
            dt = datetime.fromtimestamp(send_time)
            config['day'] = dt.day
            config['month'] = dt.month
            config['weekday'] = dt.strftime('%A')  
            config['hour'] = dt.hour  
        if dt.hour == now.hour:
            period = config.get('period')
            if period == 'once':
                if dt.date() == now.date():  
                    send(config)
            elif period == 'daily':
                send(config)
            elif period == 'weekly':
                if dt.weekday() == now.weekday(): 
                    send(config)
            elif period == 'monthly':
                if dt.day == now.day:  
                    send(config)



def send(config):
    context = config['context']
    df_registernobours = fillter_registernobours(config['config']['nobours'])
    df_insurance = fillter_insurance(config['config']['insurance'])
    df = pd.concat([df_registernobours,df_insurance])
    df['result'] = df.apply(replace_placeholders, args=(context,), axis=1)
    df = df[['شماره تماس','result']]
    df = df.to_dict('records')
    for i in df:
        now = datetime.now()
        if now.hour <= 21:
            # SendSms(i['result'],i['شماره تماس'])
            SendSms(i['result'],"09011010959")
            SendSms(i['result'],"09037976393")
            break


while True:
    check_config ()
    sleep(60*30)



