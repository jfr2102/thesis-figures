import pandas as pd
from input import *
from datetime import datetime
from datetime import timedelta
path = "/home/jfr/Thesis/kafka-logs/"
group_size = 5
def Average(lst):
    return sum(lst) / len(lst)

def rounded_percent(a, b):
    if a == b:
        return 0
    else:
        return round((((b - a) / a) * 100), 2)


run_groups = list(zip(*(iter(runs),) * group_size))
for run_group in run_groups:
    check_group = pd.DataFrame()
    benchmark_group = pd.DataFrame()
    check_group_tp = []
    benchmark_group_tp = []

    first_run = run_group[0].split("_")
    #run_title = "_".join(first_run[-2:])
    run_title = first_run[-1]

    for run in run_group:
        filename = path + run + "/resultsoutput.csv"
        producerfilename = path + run + "/produceroutput.csv"
        testdriverfilename = path + run + "/testdriverinfo.txt"

        df = pd.read_csv(filename, sep=";", parse_dates=True,
                         infer_datetime_format=True)
        df[' record.timestamp'] = df[' record.timestamp'].apply(
            lambda ts: pd.to_datetime(ts, unit='ms').to_pydatetime())

        df[' latency'] = df['latencylocal']

        check = (df.groupby(["note"]).get_group(" check")).head(-12)
        check_group = check_group.append(check)
        event_count_check = check[" window_size"].sum()

        benchmark = (df.groupby(["note"]).get_group(" benchmark")).head(-6)
        benchmark_group = benchmark_group.append(benchmark)
        event_count_benchmark = benchmark[" window_size"].sum()

        producerlog = pd.read_csv(producerfilename, sep=";")
        benchmark_begin_loaded = (
            producerlog.loc[lambda df: df['note'] == "benchmark", :])['timestamp'].iloc[0]
        start_benchmark = pd.to_datetime(
            benchmark_begin_loaded, unit='ms').to_pydatetime()
        end_benchmark = benchmark[" record.timestamp"].iloc[-1]
        duration_benchmark = (end_benchmark-start_benchmark).seconds
        throughput_benchmark = event_count_benchmark / duration_benchmark
        benchmark_group_tp.append(throughput_benchmark)

        check_begin_loaded = (producerlog.loc[lambda df: df['note'] == "check", :])[
            'timestamp'].iloc[0]
        start_check = pd.to_datetime(
            check_begin_loaded, unit='ms').to_pydatetime()
        end_check = check[" record.timestamp"].iloc[-1]
        duration_check = (end_check - start_check).seconds
        throughput_check = event_count_check / duration_check
        check_group_tp.append(throughput_check)

    latency_check = check_group[" latency"]
    latency_benchmark = benchmark_group[" latency"]

    latency_check_mean = round(latency_check.mean(), 2)
    latency_benchmark_mean = round(latency_benchmark.mean(), 2)
    latency_check_max = round(latency_check.max(), 2)
    latency_benchmark_max = round(latency_benchmark.max(), 2)
    latency_check_min = round(latency_check.min(), 2)
    latency_benchmark_min = round(latency_benchmark.min(), 2)
    latency_check_median = round(latency_check.median(), 2)
    latency_benchmark_median = round(latency_benchmark.median(), 2)

    latency_delta_mean = rounded_percent(
        latency_check_mean, latency_benchmark_mean)
    latency_delta_max = rounded_percent(
        latency_check_max, latency_benchmark_max)
    latency_delta_min = rounded_percent(
        latency_check_min, latency_benchmark_min)
    latency_delta_median = rounded_percent(
        latency_check_median, latency_benchmark_median)

    # calculate total AVG TP
    check_tp_total = round(Average(check_group_tp),2)
    benchmark_tp_total = round(Average(benchmark_group_tp),2)
    tp_delta = rounded_percent(check_tp_total,benchmark_tp_total)

    print("& \\textbf{"+run_title+"}", latency_check_mean, latency_benchmark_mean, latency_delta_mean, latency_check_max, latency_benchmark_max, latency_delta_max, latency_check_min,
          latency_benchmark_min, latency_delta_min, latency_check_median, latency_benchmark_median, latency_delta_median, check_tp_total, benchmark_tp_total, tp_delta, sep=" & ", end="")
    print("\\\\")