from os import sep
from types import LambdaType
from matplotlib.ticker import Formatter
import matplotlib.ticker as mtick
import pandas as pd
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
import seaborn as sns
import matplotlib.dates as mdates
date_fmt = mdates.DateFormatter('%H:%M:%S')

#from latency import run
#################################################################
#Metrics Visualization tryout:
pattern = "%Y-%m-%d %H:%M:%S"
run="2021-11-15_19-05-12_delay_50"

metric_path="/home/jfr/Thesis/prom-metrics/"
throughputmetric = metric_path + run + "/storm_topology_Windowbolt_Throughput_sum.csv"
cpu_metric = metric_path + run + "/node_cpu_seconds_total_1m.csv"

throughput = pd.read_csv(throughputmetric, sep=",")
throughput["throughput"]=throughput["value_1"]
throughput['time'] = throughput['time'].apply(lambda ts: datetime.strptime(ts, pattern))
print(throughput)

cpu_util = pd.read_csv(cpu_metric, sep=",")
cpu_util['time'] = cpu_util['time'].apply(lambda ts: datetime.strptime(ts, pattern))

fig, axes = pyplot.subplots( figsize = ( 10, 5), nrows=1, ncols=1)
axes.xaxis.set_major_formatter(date_fmt)

#axis1.set_xticklabels(throughput["time"])
pyplot.subplot(221)

tp_ax = sns.lineplot(x="time", y="throughput", data = throughput)
tp_ax.set_ylabel("Throughput")
pyplot.subplot(222)

ax = sns.lineplot(x="time", y="value_1", data=cpu_util, label="Storm 1")
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda y, _: '{:.0%}'.format(y))) 
ax.set_ylabel("CPU Utilization")
sns.lineplot(x="time", y="value_2", data=cpu_util, label="Storm 2")
sns.lineplot(x="time", y="value_4", data=cpu_util, label="Storm 3")
sns.lineplot(x="time", y="value_3", data=cpu_util, label="Kafka")
sns.lineplot(x="time", y="value_5", data=cpu_util, label="Utilities")


pyplot.show()


