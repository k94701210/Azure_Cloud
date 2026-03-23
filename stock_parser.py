import pymssql
import time
import yfinance as yf
import schedule
from datetime import datetime

# =========================
# DB 設定
# =========================
server = "k94701210.database.windows.net"
database = "free-sql-db-2916645"
user = "dbeng"
password = "Ab123456"

# =========================
# SQL（不包含 IDENTITY）
# =========================
INS_SQL = """
INSERT INTO dbo.stocks 
(stock_id, open_price, high_price, low_price, close_price, volume, dt)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# =========================
# DB 連線（含 retry）
# =========================
def get_connection():
    for i in range(3):
        try:
            return pymssql.connect(server, user, password, database)
        except Exception as e:
            print(f"DB連線失敗，第{i+1}次重試...")
            time.sleep(5)
    raise Exception("DB連線失敗（已重試3次）")

# =========================
# 主工作（抓資料 + 寫入DB）
# =========================
def job():
    try:
        print("開始抓資料...")

        tick = yf.Ticker("2330.TW")
        info = tick.fast_info

        # 防呆（有時會是 None）
        open_price = info.open or 0
        high = info.day_high or 0
        low = info.day_low or 0
        close = info.last_price or 0
        volume = info.last_volume or 0

        now = datetime.now()

        print(f"開盤:{open_price}, 高:{high}, 低:{low}, 收:{close}, 成交量:{volume}")

        # DB寫入
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(INS_SQL, (
            2330,
            float(open_price),
            float(high),
            float(low),
            float(close),
            int(volume),
            now
        ))

        conn.commit()

        cursor.close()
        conn.close()

        print("寫入成功")

    except Exception as e:
        print(f"錯誤: {e}")

# =========================
# 排程（下午2點）
# =========================
schedule.every().day.at("14:00").do(job)

print("排程啟動...")

# =========================
# 持續執行
# =========================
while True:
    schedule.run_pending()
    time.sleep(1)