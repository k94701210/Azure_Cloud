from datetime import date
import matplotlib.pyplot as plt
import meteostat as ms

# Specify location and time range
POINT = ms.Point(35.684168086102005, 139.75743929124195)  # Try with your location
START = date(2020, 1, 1)
END = date(2024, 12, 31)

# Get nearby weather stations
stations = ms.stations.nearby(POINT, limit=4)

# Get daily data & perform interpolation
ts = ms.daily(stations, START, END)
df = ms.interpolate(ts, POINT).fetch()

# df.reset_index(inplace=True)
# df.to_csv("./weather_data.csv", index=False)

print(df)
# Plot line chart including average, minimum and maximum temperature
# df.plot(y=[ms.Parameter.TEMP, ms.Parameter.TMIN, ms.Parameter.TMAX])
# plt.show()