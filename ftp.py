from ftplib import FTP

HOST = '20.151.113.255'
USERNAME = 'pos1'  
PASSWORD = '123'

ftp = FTP()
ftp.connect(HOST,21)

print("連線成功")

ftp.login(USERNAME, PASSWORD)

print("登入成功")

ftp.cwd('pos')
files = ftp.nlst()
print("目錄下的檔案：", files)

