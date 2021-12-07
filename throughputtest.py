from math import isnan, prod
from types import LambdaType
from matplotlib.ticker import Formatter
import matplotlib.ticker as mtick
from numpy import NaN
from numpy.core.fromnumeric import partition
import pandas as pd
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
import seaborn as sns
import matplotlib.dates as mdates
from input import *

# 2 emmiter/executor inital throughput_run
#runs = ["2021-11-29_00-04-54_throughput_sustainable_1", "2021-11-29_01-03-42_throughput_sustainable_3", "2021-11-29_01-32-51_throughput_sustainable_4", "2021-11-29_16-46-48_sustainable_throughput_rerun2_"]

# 2 emitter executor/tasks granular throughput run
#runs = ["2021-11-30_13-30-01_throughput_sustainable_granular_1", "2021-11-30_17-03-52_throughput_sustainable_granular_2", "2021-11-30_15-08-49_throughput_sustainable_granular_3", "2021-11-30_15-58-01_throughput_sustainable_granular_4"]

# 6 emmiter executor/tasks, granular throughput run
#runs = ["2021-11-30_18-32-59_throughput_sustainable_granular_6_emitter_1", "2021-11-30_19-24-32_throughput_sustainable_granular_6_emitter_2", "2021-11-30_20-13-56_throughput_sustainable_granular_6_emitter_3", "2021-11-30_21-03-03_throughput_sustainable_granular_6_emitter_4"]

# 6 emitter executors/tasks 10 ms sleep time granular throughput run
#runs = ["2021-12-01_01-01-46_throughput_sustainable_granular_6_emitter_fine_1", "2021-12-01_01-51-31_throughput_sustainable_granular_6_emitter_fine_2", "2021-12-01_14-50-17_throughput_sustainable_granular_6_emitter_fine_3_rerun", "2021-12-01_03-31-05_throughput_sustainable_granular_6_emitter_fine_4"]

# 6 emitter 10 ms granular: (war vmtl aus mistake kein localshuffle grouping?)
#runs = ["2021-12-01_18-29-06_throughput_sustainable_6_emitter_10ms_sleeep_1", "2021-12-01_19-18-55_throughput_sustainable_6_emitter_10ms_sleeep_2", "2021-12-01_20-08-39_throughput_sustainable_6_emitter_10ms_sleeep_3", "2021-12-01_20-58-17_throughput_sustainable_6_emitter_10ms_sleeep_4"]

# 6 emitter 10 ms localg ropuping grob:
# runs = ["2021-12-01_23-44-54_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_grob_1", "2021-12-02_00-36-02_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_grob_2",
#         "2021-12-02_01-25-16_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_grob_3", "2021-12-02_11-19-37_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_grob_4"]

# 6 emitter 10 ms localgropuping granular:
runs = ["2021-12-02_12-08-48_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_1",
         "2021-12-02_12-58-00_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_2",
         "2021-12-02_13-47-08_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_3",
         "2021-12-02_22-42-32_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_4"]

# 6 emitter 10 ms, localgrouping rerun granular:
runs = runs + ["2021-12-04_19-54-41_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_1",
"2021-12-04_21-23-25_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_3",
"2021-12-04_22-07-35_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_4",
"2021-12-04_23-04-28_throughput_sustainable_6_emitter_10ms_sleep_local_shuffle_granular_2_rerun"]


# 6 emitter 25 ms localgropuping grob:
# runs = ["2021-12-02_15-55-58_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_grob_1", "2021-12-02_16-44-57_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_grob_2",
#        "2021-12-02_17-34-07_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_grob_3", "2021-12-02_18-23-15_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_grob_4"]

# 6 emitter 25 ms localgropuping granular:
# runs = ["2021-12-02_19-12-23_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_granular_1", "2021-12-02_20-01-30_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_granular_2",
#         "2021-12-02_20-50-45_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_granular_3", "2021-12-02_21-39-44_throughput_sustainable_6_emitter_25ms_sleep_local_shuffle_granular_4"]


def annotate_interval():
    ticks = list(zip(x_ticks, end_ticks, sns.color_palette()))
    for start, end, color in ticks:
        pyplot.axvline(x=start, color=color, linestyle=':')
        pyplot.axvline(x=end, color=color, linestyle=':')


def set_ticks(ax, x_ticks, tick_labels):
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(tick_labels)
    ax.set_xlim(x_ticks[0] - timedelta(seconds=30),
                end_ticks[-1] + timedelta(seconds=30))


def printTable(df):
    #print(df.groupby("interval").mean().round(2))
    for row in df.groupby("interval").mean().round(2).iterrows():
        interval = row[0]
        rowvalues = row[1]
        print(int(interval), int(rowvalues.msg_count), int(rowvalues.sleep_short) , int(rowvalues.sleep_factor), rowvalues.avg_latency, rowvalues.avg_throughput, sep = " & ")

def renameNotes(note):
    if len(note) == 18:
        new_id = "0" + note[-1]
        return note[0:17] + new_id
    else:
        return note

date_fmt = mdates.DateFormatter('%H:%M:%S')
result_df = pd.DataFrame({'interval': [],
                          'msg_count': [],
                          'sleep_short': [],
                          'sleep_factor': [],
                          'avg_latency': [],
                          'avg_throughput': []}
                         )

for run in runs:
    filename = path + run + "/resultsoutput.csv"
    producerfilename = path + run + "/produceroutput.csv"
    testdriverfilename = path + run + "/testdriverinfo.txt"

    producerlog = pd.read_csv(producerfilename, sep=";")
    producerlog = producerlog[(producerlog.note != "warmup")]
    producerlog = producerlog[["msg_count", "sleep_short", "sleep_factor"]]

    sns.set()
    pyplot.subplots(3, 1, figsize=(12, 15))
    pyplot.subplot(311)
    df = pd.read_csv(filename, sep=";")[[" record.timestamp", "latencylocal",
             " partition", "note", " window_size"]]
    # ignore last 2 windows for each partition (-> 12 in total here)
    last_relevant = df.tail(12).head(6)
    df = df.drop(df.groupby("note").tail(12).index, axis=0)
    df = df.append(last_relevant)
        
    df = df[(~ pd.isnull(df.note))]
    df = df[df.note != " warmup"]
    df.note = df.note.apply(renameNotes)
    # TODO: maybe muss nun in den Throughput und cpu graphen entsprechend der scope angepasst werden damit warmup hier ignored wird, vmtl nicht wegen proper x_ticks
    df[" record.timestamp"] = df[' record.timestamp'].apply(
        lambda ts: pd.to_datetime(ts, unit='ms').to_pydatetime())

    
    df_indexed = df.set_index(pd.DatetimeIndex(df[" record.timestamp"]))
    latency = df_indexed.sort_index()
    latency = latency[[" record.timestamp", "latencylocal"]]
    avg_latency = latency.resample("5s").mean()

    # ax = sns.lineplot(x=" record.timestamp", y="latencylocal", data =df, hue = " partition", palette = sns.color_palette()[0:6])
    ax = sns.lineplot(x=" record.timestamp",
                      y="latencylocal", data=avg_latency)
    ax.set_ylabel("Latency [ms]")
    ax.set_xlabel("Time")
    ax.get_xaxis().set_major_formatter(date_fmt)

    latency_per_note = df[(~ pd.isnull(df.note))].groupby("note")
    latency_notes = [latency_per_note.get_group(
        x) for x in latency_per_note.groups]
 
    x_ticks = []
    tick_labels = []
    end_ticks = []
    n = 0
    print(run)
    for note_group in latency_notes:
        begin_group = note_group.iloc[0][" record.timestamp"]
        end_group = note_group.iloc[-1][" record.timestamp"]
        x_ticks.append(begin_group)
        # +10 seconds for the previously ommited last 2 windows
        end_ticks.append(end_group + timedelta(seconds = 10))
        tick_labels.append("t_" + str(n))
        end_group = note_group.iloc[-1][" record.timestamp"]
        count_group = note_group[" window_size"].sum()
        duration_group = (end_group - begin_group).seconds
        latency_avg = note_group["latencylocal"].mean()
        interval_conf = producerlog.iloc[n]
        n += 1

        result_df_row = {'interval': (n-1), 'msg_count': interval_conf["msg_count"], 'sleep_short': interval_conf["sleep_short"],
                         'sleep_factor': interval_conf["sleep_factor"], 'avg_latency': latency_avg, 'avg_throughput': count_group/duration_group}
        result_df = result_df.append(result_df_row, ignore_index=True)

        # print(n-1
        #     , interval_conf["msg_count"]
        #     , interval_conf["sleep_short"]
        #     , interval_conf["sleep_factor"]
        #     , round(latency_avg,2)
        #     , round(count_group/duration_group, 2)
        #    # , duration_group
        #     , sep = "&"
        #    )

    annotate_interval()
    set_ticks(ax,  x_ticks, tick_labels)

    pattern = "%Y-%m-%d %H:%M:%S"
    metric_path = "/home/jfr/Thesis/prom-metrics/"
    throughput_sum_file = metric_path + run + \
        "/storm_topology_Windowbolt_Throughput_sum.csv"
    throughput_file = metric_path + run + "/storm_topology_Windowbolt_Throughput.csv"
    cpu_file = metric_path + run + "/node_cpu_seconds_total_1m.csv"

    throughput_sum = pd.read_csv(throughput_sum_file, sep=",")
    throughput_sum["throughput"] = throughput_sum["value_1"]
    throughput_sum['time'] = throughput_sum['time'].apply(
        lambda ts: datetime.strptime(ts, pattern))

    throughput = pd.read_csv(throughput_file, sep=",")
    throughput['time'] = throughput['time'].apply(
        lambda ts: datetime.strptime(ts, pattern))

    cpu_util = pd.read_csv(cpu_file, sep=",")
    cpu_util['time'] = cpu_util['time'].apply(
        lambda ts: datetime.strptime(ts, pattern))

    pyplot.subplot(312)
    ax_tp = sns.lineplot(x="time", y="throughput", data=throughput_sum)
    ax_tp.set_ylabel("Throughput")
    ax_tp.get_xaxis().set_major_formatter(date_fmt)
    set_ticks(ax_tp, x_ticks, tick_labels)
    annotate_interval()

    pyplot.subplot(313)

    ax_cpu = sns.lineplot(x="time", y="value_1",
                          data=cpu_util, label="Storm 1")
    ax_cpu.yaxis.set_major_formatter(
        mtick.FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
   
    ax_cpu.set_ylabel("CPU Utilization")
    ax_cpu.get_xaxis().set_major_formatter(date_fmt)
 
    set_ticks(ax_cpu, x_ticks, tick_labels)

    sns.lineplot(x="time", y="value_2", data=cpu_util, label="Storm 2")
    sns.lineplot(x="time", y="value_5", data=cpu_util, label="Storm 3")
    sns.lineplot(x="time", y="value_3", data=cpu_util, label="Kafka")
    sns.lineplot(x="time", y="value_4", data=cpu_util, label="Utilities")
    annotate_interval()
    pyplot.legend(loc = "upper left", title = "VM")
    pyplot.savefig(run + "_sustainable_throughput.pdf", bbox_inches='tight')
    # pyplot.show()
printTable(result_df)
