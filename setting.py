from pymongo import MongoClient

# آدرس سرور MongoDB
host = '192.168.11.13'
# پورت پیش‌فرض MongoDB
port = 27017


ip_sql_server = '192.168.10.5'
username_sql_server = 'findev'
password_sql_server = 'Moeen....6168'
port_sql_server = '50068'

client = MongoClient(host, port)
farasahmDb = client['farasahm2']
pishkarDb= MongoClient('192.168.11.11' , port)
pishkarDb = pishkarDb['pishkar']

rest_api_token = 'ZtqX2dtvjxyYwnjInl8xGhGiynj5uKiO'





