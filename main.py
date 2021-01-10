import requests # библиотека для запроса данных https
import csv
import json
import datetime
from datetime import timedelta
import time


def get_data_feeds(): #
    f = open('data.csv', 'w+')  # создаем файл если его нет (следить, чтобы уходило из памяти в конце)
    column_names = False

    config = open("config.json")
    config_json = json.loads(config.read())

    headers = {"Authorization": config_json["key"]}
    # bearer = GTT authorisation

    url = "https://api.tardis.dev/v1/data-feeds/bitmex"

    date_time_str = '2020-08-14 00:00:00.000000'
    date_time_obj = datetime.datetime.strptime(date_time_str,'%Y-%m-%d %H:%M:%S.%f')

    print(date_time_obj.date())

    filters = [    #cписок есть data type https://docs.tardis.dev/historical-data-details/bitmex
        {"channel": "funding", "symbols": ["XBTUSD", "ETHUSD", "LTCUSD", "XRPUSD", "BCHUSD"]},
        #{"channel": "tradeBin1d", "symbols": ["XBTUSD", "ETHUSD", "LTCUSD", "XRPUSD", "BCHUSD"]},
    ]

    # unique_tickers = {}
    # csv_file = csv.writer(f)

    # while date_time_obj < (datetime.datetime.now() - timedelta(days=1)): # цикл
    while date_time_obj != datetime.datetime.now():

        qs_params = {"from": date_time_obj.date(), "offset": 1440, "filters": json.dumps(filters)}
        response = requests.get(url, headers=headers, params=qs_params, stream=True)
        print(response.content)
        date_time_obj = date_time_obj + timedelta(days=1)

        # while offset <= 1440:
        #      offset = offset + 1

        for line in response.iter_lines():
            # empty lines in response are being used as markers
            # for disconnect events that occurred when collecting the data
            if len(line) <= 1:
                continue

            parts = line.decode("utf-8").split(" ")
            local_timestamp = parts[0] #различает меседжи по временной метке
            message = json.loads(parts[1])
            # local_timestamp string marks message arrival timestamp
            # message is a message dict as provided by exchange real-time stream
            # print(local_timestamp, message) – см образец данных

              # обертка для файла чтобы превратить его в csv

            csv_file = csv.writer(f)

            for item in message["data"]:
                if column_names == False: # == compare тоже что и is / = присваивание
                    csv_file.writerow(item.keys())
                    column_names = True

                csv_file.writerow(item.values())


    #             if message["table"] == "funding":
    #                 new = unique_tickers.get(list(item.values())[1], [])
    #                 unique_tickers[list(item.values())[1]] = list(item.values()) + new
    #                 continue
    #             unique_tickers[list(item.values())[1]] = list(item.values())
    #
    # for k, v in unique_tickers.items():
       # csv_file.writerow(v)
        # time.sleep(1) #sleep 1 sec

    f.close()
    config.close() #чтобы не перегружать память


get_data_feeds()



