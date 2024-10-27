from setting import ip_sql_server, port_sql_server, password_sql_server, username_sql_server
import pyodbc
import pandas as pd

database = 'ACE_ACC5301403'
ACC = 'ACC'
PRS = 'PRS'
conn_str_db = f'DRIVER={{SQL Server}};SERVER={ip_sql_server},{port_sql_server};DATABASE={database};UID={username_sql_server};PWD={password_sql_server}'
conn = pyodbc.connect(conn_str_db)
cursor = conn.cursor()

# گرفتن نام همه جداول
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
tables = cursor.fetchall()

query = f"SELECT * FROM PRS"
df_prs = pd.read_sql(query, conn)
query = f"SELECT * FROM ACC"
df_ACC = pd.read_sql(query, conn)
df_ACC = df_ACC[df_ACC['Code1']=='      110']
df_ACC = df_ACC[df_ACC['Code2']=='       03']
df_ACC = df_ACC[df_ACC['Code3']!=None]

for i in df_prs.index:
    CodePer = df_prs['Code'][i]
    if int(CodePer)<30000:
        continue
    Code = '110-03-'+str(CodePer)
    if Code in df_ACC['Code']:
        continue
    
    
    
    dic = {
        'Code':Code,
        'Code1':'110',
        'Code2':'03',
        'Code3':str(CodePer),
        'Code4':None,
        'Code5':None,
        'Name':df_prs['Name'][i],
        'LtnName':None,
        'Gru_Code':None,
        'ParDar':None,
        'DarMode':0,
        'BedBes':None,
        'Bank':False,
        'HasChild':False,
        'AccLevel':0,
        'Arzi':False,
        'Arz_Code':None,
        'Meghdar':None,
        'Vahed':None,
        'Deghat_m':0,
        'Hazineh':None,
        'Mkz_Code':None,
        'Opr':False,
        'Opr_Code':None,
        'Spec':'ايجاد اتوماتيك از سيستم حسابداري',
        'PrsCode':str(CodePer),
        'PGruCode':None,
        'Atf':False,
        'Email':None,
        'Owner':None,
        'F01':None,
        'F02':None,
        'F03':None,
        'F04':None,
        'F05':None,
        'F06':None,
        'F07':None,
        'F08':None,
        'F09':None,
        'F10':None,
        'F11':None,
        'F12':None,
        'F13':None,
        'F14':None,
        'F15':None,
        'F16':None,
        'F17':None,
        'F18':None,
        'F19':None,
        'F20':None,
        'AccStatus':0,
        'UpdateUser':'ACE',
        'UpdateDate': pd.Timestamp.now(),  # تاریخ و زمان فعلی
        'Branch':0,
        'Mobile':df_prs['Mobile'][i],
        'TempHasChild':0,
        'AccComm':None,
        'AccStatusComm':None,
        'Users':None,
        'IOprI':False,
        'IOprI_Code':None,
        'IPrsI':False,
        'IPrsI_Code':None,

    }
    cursor.execute("SELECT COUNT(*) FROM ACC WHERE Code = ?", Code)
    if cursor.fetchone()[0] > 0:
        continue  # اگر رکورد وجود دارد، ادامه بده
    columns = ', '.join(dic.keys())
    placeholders = ', '.join('?' * len(dic))
    insert_query = f"INSERT INTO ACC ({columns}) VALUES ({placeholders})"
    print(i)
    
    # اجرای دستور INSERT
    cursor.execute(insert_query, list(dic.values()))
    conn.commit()





# بستن اتصال به دیتابیس
conn.close()