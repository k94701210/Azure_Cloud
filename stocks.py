import pymssql
import time
import yfinance as yf
import schedule
from datetime import datetime
import threading

# =========================
# DB 設定
# =========================
server = "k94701210.database.windows.net"
database = "free-sql-db-2916645"
user = "dbeng"
password = "Ab123456"

# =========================
# SQL（單一表）
# =========================
INS_SQL = """
INSERT INTO dbo.stocks
(stock_id, open_price, high_price, low_price, close_price, volume, dt)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# =========================
# DB 連線（retry）
# =========================
def get_connection():
    for i in range(3):
        try:
            return pymssql.connect(server, user, password, database)
        except:
            print(f"DB連線失敗，第{i+1}次重試...")
            time.sleep(5)
    raise Exception("DB連線失敗")

# =========================
# 單一股票處理
# =========================
def fetch_and_save(symbol, stock_id):
    try:
        print(f"開始抓 {symbol}...")

        tick = yf.Ticker(symbol)
        info = tick.fast_info

        # 防呆
        if info.last_price is None:
            print(f"{symbol} 今日無資料（可能休市）")
            return

        open_price = info.open or 0
        high = info.day_high or 0
        low = info.day_low or 0
        close = info.last_price or 0
        volume = info.last_volume or 0

        now = datetime.now()

        print(f"{symbol} → 開:{open_price}, 高:{high}, 低:{low}, 收:{close}")

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(INS_SQL, (
            stock_id,
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

        print(f"{symbol} 寫入成功")

    except Exception as e:
        # 防止 UNIQUE constraint 報錯炸掉
        if "duplicate" in str(e).lower():
            print(f"{symbol} 今日資料已存在")
        else:
            print(f"{symbol} 錯誤: {e}")

# =========================
# 多執行緒 job
# =========================
def job():
    print("開始多股票抓取...")

    stocks = [
        ("2330.TW", "2330"),
        ("^GSPC", "GSPC"),
        ("AAPL", "AAPL")
    ]

    threads = []

    for symbol, sid in stocks:
        t = threading.Thread(target=fetch_and_save, args=(symbol, sid))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("全部完成")

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