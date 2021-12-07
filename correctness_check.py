import pandas as pd
import json
import os
import sys
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
    if len(correctness_result.index) == 0:
        print("\tequalMessagePerWindow Test Failed:", "Empty check DataFrame")
        return False
    for row in benchmark_result.iterrows():
        row_value = row[1]
        record_key = row_value[" record.key"]
        partition = row_value[" partition"]
        window_size = row_value[" window_size"]
        matches = correctness_result[(correctness_result[" record.key"] == record_key)
                                     & (correctness_result[" partition"] == partition)
                                     & (correctness_result[" window_size"] == window_size)]

        if len(matches) != 1:
            print("\tequalMessagePerWindow Test Failed:",
                  record_key, partition, window_size)
            return False
    return True


def equalRecordValue(df):
    benchmark_result, correctness_result = result_split(df)
    if len(correctness_result.index) == 0:
        print("\teequalRecordValue failed:", "Empty check DataFrame")
        return False
    for row in correctness_result.iterrows():
        row_value = row[1]
        record_key = row_value[" record.key"]
        partition = row_value[" partition"]
        record_value = row_value[" record.value"]
        count_per_city = json.loads(record_value)["count_per_city"]

        benchmark_result["count_per_city"] = benchmark_result[" record.value"].apply(
            lambda x: json.loads(x)["count_per_city"])
        matches = benchmark_result[(benchmark_result[" record.key"] == record_key)
                                   & (benchmark_result[" partition"] == partition)
                                   & (benchmark_result["count_per_city"] == count_per_city)
                                   ]
        if len(matches) != 1:
            print("\tequalRecordValue Test Failed:",
                  record_key, partition, count_per_city)
            return False
    return True

def testFun():
    return True
    return False

for run in runs:

    try:
        os.mkdir("outputs/"+run+"/")
    except FileExistsError:
        pass
    out = open('outputs/' + run + '/correctness_output.txt', 'w')

    print(run, ":")
    filename = path + run + "/resultsoutput.csv"
    df = pd.read_csv(filename, sep=";")
    # Number of messages equal?
    print("Equal message count total:", equalMessageTotal(df),
    "\nEqual message count per window:", equalMessagePerWindow(df),
    "\nEqual message count per city per window:", equalRecordValue(df),
    file = out)
    print(testFun())
