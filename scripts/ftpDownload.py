import ftplib

# Set the FTP details
path  = 'pub/data/ghcn/daily/by_year/'
filename = '2017.csv.gz'
ftpWebsite = "ftp.ncdc.noaa.gov"
userName = "anonymous"
password = "foo@gmail.com"

ftp = ftplib.FTP(ftpWebsite) 

ftp.login(user=userName, passwd=password) 
ftp.cwd(path)

localfile = open(filename, 'wb')
ftp.retrbinary("RETR " + filename, localfile.write)
ftp.quit()
localfile.close()
