import pymssql
import random
import time

"""
使用 pymssql 對資料庫進行連接
"""
server = "k94701210.database.windows.net"
database = "free-sql-db-2916645"
user = "dbeng"
password = "Ab123456"   
'''密碼洩漏問題'''

INS_SQL = """
    INSERT into dbo.stocks(sid,sname,price) 
    VALUES (
        %s,
        %s,
        %d
    )
"""
try:
    connect = pymssql.connect(server, user, password, database)
    cursor = connect.cursor()


    for i in range(1,30):
        # 產生亂數 介於( 1850~ 1905)
        rp = random.randint(1850,1905)
        cursor.execute(INS_SQL, ("2330","TSMC",rp))
        # pymssql 預設設定 autocommit = false
        connect.commit()
        print(f'第{i}次擷取,金額 {rp}')
        if( i < 30 ):  
            time.sleep(3)   # 如果還在回圈內就 休眠 5秒



    print("資料寫入完畢")
    cursor.close()
    connect.close()




except Exception as e: 
    print(f'連線失敗: 原因{e}')
