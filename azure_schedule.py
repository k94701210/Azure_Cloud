from datetime import datetime
import time
import schedule

def sayhello():
    now = datetime.now()
    print("Hello! The current time is: ", now)
schedule.every(5).seconds.do(sayhello)
while True:
    schedule.run_pending()
    time.sleep(1)