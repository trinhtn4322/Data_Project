# PART 1
## In this part 1 we will use the necessary skills of a data engineer
- Includes: Python, HTML understanding, ETL pipeline building, tools like: Hadoop, Spark

- Some libraries I used like: requests, BeautifulSoup, csv, datetime, pathlib, os, pandas, pyspark

- Data is taken directly from the web: https://www.metoffice.gov.uk/
- Location: Ho Chi Minh City(Vietnam)

### The code consists of 6 parts:

- def getday is used to get the current date and the date the user wants to get, the limit of the web can be obtained is 1 week 6 days, after 6 days will continue to get new data.

- def **export** will store the data locally

- def **local** to create a path that supports export save to local

- def **check** will use python for simple data processing before saving to data lake

- def **load** will put the data on the data lake

- def **CrawlData**, the most important because it is responsible for getting data from the web and combining with the remaining def to upload to the data lake
