import os
import requests
import json
import math
import sys
from time import sleep
from datetime import datetime


exchange = sys.argv[1] if len(sys.argv) > 1 else 'bybit'
symbol=  sys.argv[2] if len(sys.argv) > 2 else 'BTCUSD'
print("crawling data from " + exchange)

try:
    os.makedirs('history/'+exchange)
except Exception:
    pass


batch_size = 50000
package_size = 200
lastSync= 0
result = []

urls = {
    "bybit": "https://api.bybit.com/v5/market/mark-price-kline?category=inverse&symbol=##symbol##&interval=1",
    "bybit-linear": "https://api.bybit.com/v5/market/mark-price-kline?category=linear&symbol=##symbol##&interval=1"
}

URL = urls[exchange].replace("##symbol##",symbol)

known_history_files= {
    "bybit_BTCUSD": -1 # no files: -1
}


def history_file_name(index, exchange, symbol=''):
    if len(symbol) > 0:
        symbol += "_"
    return './history/' + exchange + '/' + symbol + 'M1_' + str(index) + '.json'


if exchange == 'bybit':
    if symbol == 'ETHUSD':
        start = 1548633600000
    elif symbol == 'BTCUSD':
        start = 1542502800000

lastknown = known_history_files[exchange+"_"+symbol] if exchange+"_"+symbol in known_history_files else -1
if lastknown >= 0:
    try:
        with open(history_file_name(lastknown,exchange,symbol), 'r') as file:
            result = json.load(file)
            start = int(result[-1][0])+60000
    except Exception as e:
        print("Could not find history files ("+str(e)+")")

while True:
    # send request
    if start > int(datetime.now().timestamp() * 1000):
        print("I am not an oracle. All price data pulled. Good luck! ")
        break
    url = URL + "&start=" + str(start) + "&end=" + str(start+package_size*60*1000)
    print(url+" __ "+str(len(result)))
    r = requests.get(url=url)

    # extract data in json format
    jsonData = r.json()
    data = jsonData["result"]['list']
    data.reverse()

    # update loop variables
    result += data
    lastSync += len(data)
    start = int(data[-1][0]) + 60000
    package_complete = len(data) >= package_size
    print(datetime.fromtimestamp(int(data[-1][0]) / 1000))

    # writing to file
    if lastSync > 15000 or not package_complete:
        lastSync = 0
        max = math.ceil((len(result)) / batch_size)
        idx = max - 2   # write to the last two files
        while idx < max:
            if idx*batch_size >= 0:
                with open(history_file_name(idx,exchange,symbol),'w') as file:
                    content = result[idx * batch_size:(idx + 1) * batch_size]
                    json.dump(content,file)
                    print("wrote to file "+str(idx))
            idx += 1

    if not package_complete:
        print('Received less data than expected: ' + str(len(data)) + ' entries')
        print("Short break. Will continue shortly after.")
        sleep(5)








