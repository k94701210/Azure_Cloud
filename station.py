from datetime import date
import matplotlib.pyplot as plt
import meteostat as ms
     

loc =ms.Point(24.180078828331514, 120.69087036872334)
king =ms.Point(35.68287835792772, 139.75455230254664)

slist=ms.stations.nearby(king, limit=4)

print("在指定座標附近的偵測點如下:")
print(slist)