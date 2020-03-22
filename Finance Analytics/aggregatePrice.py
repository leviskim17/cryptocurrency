
import requests
import csv

url = "https://api.blockchain.info/charts/market-price?timespan=20weeks&rollingAverage=8hours&format=csv"

Data = requests.request("GET", url)
print(Data.text)

csvFile = open('price.csv', 'w')
csvWriter = csv.writer(csvFile)

templeteYear = '2018-04-30'
templeteHour = 4
templeteMin = 11
lines = Data.text.splitlines()
for line in lines:
    words = line.split(',')
    if(templeteMin == 59):
        templeteHour += 1
        templeteMin = 0
    else:
        templeteMin += 1
    
    time = templeteYear + ' '  +str(templeteHour).zfill(2) + ':' + str(templeteMin).zfill(2)  + ':00'
    csvWriter.writerow([time, words[1]])

csvFile.close()



