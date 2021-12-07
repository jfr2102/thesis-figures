import pandas as pd
import json
from input import *
pd.options.mode.chained_assignment = None
path = "/home/jfr/Thesis/kafka-logs/"


def result_split(df):
    return df[~ df.note.str.contains("_correctness_topology")], df[df.note.str.contains("_correctness_topology")]


def equalMessageTotal(df):
    benchmark_result, correctness_result = result_split(df)
    return len(benchmark_result) == len(correctness_result)


def equalMessagePerWindow(df):
    benchmark_result, correctness_result = result_split(df)
    for row in benchmark_result.iterrows():
        row_value = row[1]
        record_key = row_value[" record.key"]
        partition = row_value[" partition"]
        window_size = row_value[" window_size"]
        matches = correctness_result[(correctness_result[" record.key"] == record_key)
                                     & (correctness_result[" partition"] == partition)
                                     & (correctness_result[" window_size"] == window_size)]

        if len(matches) != 1:
            return False
    return True

def equalRecordValue(df):
    benchmark_result, correctness_result = result_split(df)
    for row in benchmark_result[0:12].iterrows():
        row_value = row[1]
        record_key = row_value[" record.key"]
        partition = row_value[" partition"]
        record_value = row_value[" record.value"]
        count_per_city = json.loads(record_value)["count_per_city"]

        correctness_result["count_per_city"] = correctness_result[" record.value"].apply(lambda x: json.loads(x)["count_per_city"])
        matches = correctness_result[(correctness_result[" record.key"] == record_key)
                                     & (correctness_result[" partition"] == partition)
                                     & (correctness_result["count_per_city"] == count_per_city)
                                    ]
        if len(matches) != 1:
            return False
    return True

for run in runs:
    print(run)
    filename = path + run + "/resultsoutput.csv"
    df = pd.read_csv(filename, sep=";")
    # Number of messages equal?
    print("Equal message count total:", equalMessageTotal(df))
    print("Equal message count per window:", equalMessagePerWindow(df))
    print("Equal message count per city per window:", equalRecordValue(df))