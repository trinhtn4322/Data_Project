import requests
from bs4 import BeautifulSoup
import csv
import datetime
from pathlib import Path
import os
import pandas as pd
from pyspark.sql import SparkSession

requests, BeautifulSoup, csv, datetime, pathlib, os, pandas, pyspark,
def getday():
    today = datetime.date.today()

    year = today.year
    month = today.month
    day = today.day
    while True:
        day_end = int(input('Enter day end, limit in 6 days::'))
        if day_end < 7:
            break
    month=str(month).zfill(2)
    day_end= day + day_end
    return day_end, day, month, year


def CrawlData(day_end, days, month, year):
    output_rows = []
    for day in range(days, day_end):
        date = str(year) + '-' + str(month) + '-' + str(day)

        url = 'https://www.metoffice.gov.uk/weather/forecast/w3gve307f#?date=' + date

        # Gửi yêu cầu GET để lấy nội dung HTML
        response = requests.get(url)
        html_content = response.text

        # Phân tích cú pháp HTML bằng BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        daytime=soup.find('div',id=date)
        time = daytime.find('tr', class_='step-time')
        hours = []
        for hour in time:
            if hour != '\n':
                hours.append(hour.text)
                hours = [value.strip()
                         for value in hours]
        dayz = []
        for i in range (1,len(hours)):
            dayz.append(date+' '+hours[i])
        output_rows.append(dayz)
        table = soup.find('div', id=date)
        for row in table.findAll('tr'):
            cols = row.findAll('td')
            output_row = []
            for col in cols:
                if col.text != '\n\n':
                    output_row.append(col.text)
                    output_row = [value.strip().replace('%', '').replace('\n', ' ').replace('<', ' ').replace('°',' ').replace('-', '0') for value in output_row]

            if len(output_row) > 0:
                output_rows.append(output_row)

        fix = output_rows.pop(4)
        # fix2= output_rows.pop(7)
        print(output_rows)
        split_data = [value.split() for value in fix]
        column1 = [value[0] for value in split_data]
        column2 = [value[1] for value in split_data]


        output_rows.append(column1)
        output_rows.append(column2)
        print(output_rows)
        name_csv = 'output_weather_in_' + date + '.parquet'
        name_file='output_weather_in_' + date
        print('Name:',name_csv)
        path = local(year, month, name_file)
        export(path,output_rows)
        check(path)
        load(year,month,name_file,path)
        output_rows=[]



def export(path, output_rows):
    dir_path = os.path.dirname(path)
    os.makedirs(dir_path, exist_ok=True)
    df = pd.DataFrame(output_rows)
    df = df.transpose()
    df.to_parquet(path)


def load(year, month,dataset_file,path):
    spark = SparkSession.builder.config('spark.port.maxRetries', 100).appName("Load CSV to Hadoop").getOrCreate()
    path = f"{path}"
    hdfs_parquet_path = f"hdfs://localhost:9000/datalake/{year}/{month}/{dataset_file}.parquet"
    df = spark.read.parquet(path)
    df.show()
    print(path)
    df.write.format("parquet").mode("overwrite").save(hdfs_parquet_path)


def local(year, month, dataset_file:str) -> Path:
    path = Path(f"hdfs://localhost:9000/datalake/{year}/{month}/{dataset_file}.parquet")
    path=Path(f"datalake/{year}/{month}/{dataset_file}.parquet")
    print('Path:',path)
    return path

def check(path):
    df = pd.read_parquet(path)
    df[0] = pd.to_datetime(df[0])
    df[1] = df[1].astype(int)
    df[2] = df[2].astype(int)
    df[3] = df[3].astype(int)
    df[4] = df[4].astype(int)
    df[6] = df[6].astype(int)
    df[7] = df[7].astype(int)
    df[9] = df[9].astype(int)
    df.columns = ['Date', 'Chance of precipitation', 'Temperature', 'Feels like temperature °C', 'Wind gust mph',
                  'Visibility', 'Humidity.', 'UV .', 'Wind direction', 'Speed']
    df.to_parquet(path, index=False)

if __name__ == "__main__":
    day_end, days, month, year=getday()
    CrawlData(day_end, days, month, year)
