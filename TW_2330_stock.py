import pymssql
import time
import yfinance as yf
import schedule
from datetime import datetime
import threading
import random

# =========================
# DB 設定
# =========================
server = "k94701210.database.windows.net"
database = "free-sql-db-2916645"
user = "dbeng"
password = "Ab123456"

# =========================
# SQL 指令
# =========================
INS_SQL = """
INSERT INTO dbo.TW_2330_stocks 
(stock_id, open_price, high_price, low_price, close_price, volume, dt)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# =========================
# DB 連線（含隨機重試機制）
# =========================
def get_connection():
    for i in range(3):
        try:
            # 加上 login_timeout 避免程式卡死在連線階段
            return pymssql.connect(server, user, password, database, login_timeout=10)
        except Exception as e:
            # 加入隨機抖動 (Jitter)，避免多個執行緒同時重試造成二度碰撞
            wait_time = 5 + random.uniform(1, 3)
            print(f"DB連線失敗({e})，第{i+1}次重試，等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)
    raise Exception("DB連線最終失敗，請檢查網路或 Azure 防火牆設定")

# =========================
# 單一股票處理邏輯
# =========================
def fetch_and_save(symbol, stock_id):
    # --- 新增：啟動前隨機延遲 0~3 秒，錯開 Thread 請求峰值 ---
    time.sleep(random.uniform(0, 3))
    
    try:
        print(f"開始抓取 {symbol}...")
        
        # 1. 抓取資料 (yfinance API)
        tick = yf.Ticker(symbol)
        info = tick.fast_info

        if info.last_price is None or str(info.last_price) == 'nan':
            print(f"{symbol} 無即時資料（可能休市中）")
            return

        # 整理欄位
        open_p = float(info.open or 0)
        high_p = float(info.day_high or 0)
        low_p  = float(info.day_low or 0)
        close_p = float(info.last_price or 0)
        vol    = int(info.last_volume or 0)
        now    = datetime.now()

        print(f"{symbol} 數據：開:{open_p}, 收:{close_p}")

        # 2. 存入資料庫
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(INS_SQL, (stock_id, open_p, high_p, low_p, close_p, vol, now))
            conn.commit()
            print(f"[{symbol}] 寫入成功 @ {now.strftime('%H:%M:%S')}")
        except Exception as db_e:
            # 判斷是否為重複寫入 (UNIQUE Constraint)
            if "duplicate" in str(db_e).lower():
                print(f"[{symbol}] 資料已存在，跳過寫入")
            else:
                print(f"[{symbol}] 資料庫寫入失敗: {db_e}")
        finally:
            if conn:
                conn.close() # 確保連線一定會釋放

    except Exception as e:
        print(f"[{symbol}] 抓取過程發生非預期錯誤: {e}")

# =========================
# Job 排程任務
# =========================
def job():
    print(f"\n--- 執行定時任務: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

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

    print("--- 本次排程任務全部結束 ---\n")

# =========================
# 主程式執行入口
# =========================
if __name__ == "__main__":
    print("雲端股票監控程式啟動中...")
    
    # 測試執行一次
    job()

    # 設定每 30 分鐘執行一次
    schedule.every(30).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)