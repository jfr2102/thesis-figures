from math import isnan
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

def annotate_interval():
    for tick in x_ticks:
        pyplot.axvline(x=tick, color='g', linestyle=':')
def set_ticks(ax, x_ticks, tick_labels):
   ax.set_xticks(x_ticks)
   ax.set_xticklabels(tick_labels)
   ax.set_xlim(x_ticks[0] - timedelta(seconds = 30) , x_ticks[-1] + timedelta(minutes = 3))
        
run = "2021-11-24_14-41-57_throughput_3_"

date_fmt = mdates.DateFormatter('%H:%M:%S')

filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"
sns.set()

pyplot.subplots(3,1,figsize=(7,12))
pyplot.subplot(311)
df = pd.read_csv(filename, sep=";")

df = df[[" record.timestamp", "latencylocal", " partition", "note", " window_size"]]
df = df[( ~ pd.isnull(df.note))]

df[" record.timestamp"] = df[' record.timestamp'].apply(lambda ts: pd.to_datetime(ts, unit='ms').to_pydatetime())
df_indexed = df.set_index(pd.DatetimeIndex(df[" record.timestamp"]))
latency = df_indexed.sort_index()
latency = latency[[" record.timestamp", "latencylocal"]]
avg_latency = latency.resample("5s").mean()

# ax = sns.lineplot(x=" record.timestamp", y="latencylocal", data =df, hue = " partition", palette = sns.color_palette()[0:6])
ax = sns.lineplot(x=" record.timestamp", y="latencylocal", data = avg_latency)
ax.set_ylabel("Latency [ms]")
ax.set_xlabel("Time")
ax.get_xaxis().set_major_formatter(date_fmt)


latency_per_note = df[( ~ pd.isnull(df.note))].groupby("note")    
latency_notes =[ latency_per_note.get_group(x) for x in latency_per_note.groups]

x_ticks = []
tick_labels = []
n = 1
for note_group in latency_notes:
    begin_group = note_group.iloc[0][" record.timestamp"]
    x_ticks.append(begin_group)
    tick_labels.append("t_" + str(n))
    n += 1
    pyplot.axvline(x=begin_group, color='g', linestyle=':')

    end_group = note_group.iloc[-1][" record.timestamp"]

    count_group = note_group[" window_size"].sum()
    duration_group = (end_group - begin_group).seconds
    latency_avg = note_group[ "latencylocal"].mean()
    print(n-1, round(latency_avg,2), round(count_group/duration_group, 2), sep = "&")

set_ticks(ax,  x_ticks, tick_labels)

pattern = "%Y-%m-%d %H:%M:%S"
metric_path="/home/jfr/Thesis/prom-metrics/"
throughput_sum_file = metric_path + run + "/storm_topology_Windowbolt_Throughput_sum.csv"
throughput_file = metric_path + run + "/storm_topology_Windowbolt_Throughput.csv"
cpu_file = metric_path + run + "/node_cpu_seconds_total_1m.csv"

throughput_sum = pd.read_csv(throughput_sum_file, sep=",")
throughput_sum["throughput"] = throughput_sum["value_1"]
throughput_sum['time'] = throughput_sum['time'].apply(lambda ts: datetime.strptime(ts, pattern))

throughput = pd.read_csv(throughput_file, sep= ",")
throughput['time'] = throughput['time'].apply(lambda ts: datetime.strptime(ts, pattern))

cpu_util = pd.read_csv(cpu_file, sep=",")
cpu_util['time'] = cpu_util['time'].apply(lambda ts: datetime.strptime(ts, pattern))


pyplot.subplot(312)
ax_tp = sns.lineplot(x = "time", y = "throughput", data = throughput_sum)
ax_tp.set_ylabel("Throughput")
ax_tp.get_xaxis().set_major_formatter(date_fmt)
set_ticks(ax_tp, x_ticks, tick_labels)
annotate_interval()

pyplot.subplot(313)
ax_cpu = sns.lineplot(x="time", y="value_1", data=cpu_util, label="Storm 1")
ax_cpu.yaxis.set_major_formatter(mtick.FuncFormatter(lambda y, _: '{:.0%}'.format(y))) 
ax_cpu.set_ylabel("CPU Utilization")
ax_cpu.get_xaxis().set_major_formatter(date_fmt)
set_ticks(ax_cpu, x_ticks, tick_labels)

sns.lineplot(x="time", y="value_2", data=cpu_util, label="Storm 2")
sns.lineplot(x="time", y="value_5", data=cpu_util, label="Storm 3")
sns.lineplot(x="time", y="value_3", data=cpu_util, label="Kafka")
sns.lineplot(x="time", y="value_4", data=cpu_util, label="Utilities")
annotate_interval()

pyplot.savefig(run + "_sustainable_throughput.pdf")
pyplot.show()
