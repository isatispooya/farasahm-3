from pymongo import MongoClient

# آدرس سرور MongoDB
host = '192.168.11.13'
# پورت پیش‌فرض MongoDB
port = 27017

ip_sql_server = '192.168.10.5'
username_sql_server = 'findev'
password_sql_server = 'Moeen....6168'
port_sql_server = '50068'

try:
    # استفاده از URI string برای اتصال
    mongo_uri = f"mongodb://{host}:{port}/"
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    # تست اتصال
    client.server_info()
    farasahmDb = client['farasahm2']
    
    pishkar_uri = f"mongodb://192.168.11.11:{port}/"
    pishkarDb = MongoClient(pishkar_uri, serverSelectionTimeoutMS=5000)
    pishkarDb = pishkarDb['pishkar']
except Exception as e:
    print(f"خطا در اتصال به MongoDB: {e}")
    raise

rest_api_token = 'ZtqX2dtvjxyYwnjInl8xGhGiynj5uKiO'





