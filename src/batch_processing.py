from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime, timedelta
import sys
import json


def first_query(data, start_hour, end_hour):
    data = data.filter(lambda row: start_hour < datetime.fromtimestamp(int(row[5])/1000) < end_hour and row[4])\
        .map(lambda row: (row[2], [row[0]]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0], len(x[1]))).collect()

    report = {"time_start": str(start_hour.hour) + ":00", "time_end": str(end_hour.hour) + ":00", "statistics": []}
    for key, value in data:
        report["statistics"].append({key: value})

    return report


def second_query(data, start_hour, end_hour):
    data = data.filter(lambda row: start_hour < datetime.fromtimestamp(int(row[5])/1000) < end_hour and row[4])\
        .filter(lambda row: row[2] == 'us')\
        .map(lambda row: (row[3], set([row[1]])))\
        .reduceByKey(lambda a, b: a | b)\
        .collect()
    report = {"time_start": str(start_hour.hour) + ":00", "time_end": str(end_hour.hour) + ":00", "statistics": []}
    for key, value in data:
        report["statistics"].append({key: list(value)})

    return report


def third_query(data, start_hour, end_hour):
    data = data.filter(lambda row: start_hour < datetime.fromtimestamp(int(row[5])/1000) < end_hour and row[4] is not None)\
        .map(lambda row: (row[2], row[4].split(', ')))\
        .flatMapValues(lambda x: [x])\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0], max([(x[1].count(topic), topic) for topic in set(x[1])])))\
        .collect()

    report = {"time_start": str(start_hour.hour) + ":00", "time_end": str(end_hour.hour) + ":00", "statistics": []}
    for key, value in data:
        report["statistics"].append({key: {value[1]: value[0]}})

    return report


def store_report(report, outputPath):
    # df = ss.read.json(sc.parallelize([report]))
    # df.coalesce(1).write.format('json').mode("overwrite").save(outputPath)
    df = ss.read.json(sc.parallelize([report]))
    df.coalesce(1).write.format('json').mode("overwrite").save(outputPath)


if __name__ == "__main__":
    input_file = sys.argv[1]
    output_folder = sys.argv[2]

    sc = SparkContext('local[*]')
    sc.setLogLevel("ERROR")

    ss = SparkSession.builder \
        .master("master") \
        .appName("appName") \
        .getOrCreate()

    df = ss.read.format('csv') \
                .option('header',True) \
                .option('multiLine', True) \
                .load(input_file).rdd
    end_time = datetime.now() - timedelta(hours=1, minutes=datetime.now().minute)
    first_report = first_query(df, end_time - timedelta(hours=6), end_time)
    second_report = second_query(df, end_time - timedelta(hours=3), end_time)
    third_report = third_query(df, end_time - timedelta(hours=6), end_time)

    store_report(first_report, output_folder + "query1/")
    store_report(second_report, output_folder + "query2/")
    store_report(third_report, output_folder + "query3/")
