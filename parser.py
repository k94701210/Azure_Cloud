from datetime import date
import matplotlib.pyplot as plt
import meteostat as ms
import ssl
import certifi

# ✅ 正確寫法
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

POINT = ms.Point(35.684168086102005, 139.75743929124195)
START = date(2020, 1, 1)
END = date(2025, 12, 31)

stations = ms.stations.nearby(POINT, limit=4)

ts = ms.daily(stations, START, END)
df = ms.interpolate(ts, POINT).fetch()

print(df)


test