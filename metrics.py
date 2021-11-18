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
from load_meta import *
from input import *

date_fmt = mdates.DateFormatter('%H:%M:%S')
sns.set()
#################################################################
#Metrics Visualization tryout:
pattern = "%Y-%m-%d %H:%M:%S"

metric_path="/home/jfr/Thesis/prom-metrics/"
throughput_sum_file = metric_path + run + "/storm_topology_Windowbolt_Throughput_sum.csv"
throughput_file = metric_path + run + "/storm_topology_Windowbolt_Throughput.csv"
cpu_file = metric_path + run + "/node_cpu_seconds_total_1m.csv"

throughput_sum = pd.read_csv(throughput_sum_file, sep=",")
throughput_sum ["throughput"]=throughput_sum["value_1"]
throughput_sum['time'] = throughput_sum['time'].apply(lambda ts: datetime.strptime(ts, pattern))

throughput = pd.read_csv(throughput_file, sep= ",")
throughput['time'] = throughput['time'].apply(lambda ts: datetime.strptime(ts, pattern))

cpu_util = pd.read_csv(cpu_file, sep=",")
cpu_util['time'] = cpu_util['time'].apply(lambda ts: datetime.strptime(ts, pattern))

fig, axes = pyplot.subplots( figsize = (15,5))
#axes.xaxis.set_major_formatter(date_fmt)
pyplot.subplot(121)
annotate()
tp_ax = sns.lineplot(x = "time", y = "throughput", data = throughput_sum[throughput_sum.time > start_check], label = "SUM")
tp_ax.set_ylabel("Throughput")
tp_ax.get_xaxis().set_major_formatter(date_fmt)
tp_ax.set_xticks([start_check, start_benchmark, fault_end, fault_end + timedelta(minutes=0.5)])
tp_ax.set_xticklabels(["t_0", "t_1   t_2", "t_3", ""])
#tp_ax.set_xlim(left = start_check)
i = 0
for col in throughput.columns[2:]:
    i += 1
    sns.lineplot(x="time", y=col, data=throughput[throughput.time > start_check], label = "Component " + str(i))

pyplot.subplot(122)
#annotate
annotate()

cpu_util = cpu_util[cpu_util.time > start_check]
ax = sns.lineplot(x="time", y="value_1", data=cpu_util, label="Storm 1")
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda y, _: '{:.0%}'.format(y))) 
ax.set_ylabel("CPU Utilization")
ax.get_xaxis().set_major_formatter(date_fmt)
ax.set_xticks([start_check, start_benchmark, fault_end, fault_end + timedelta(minutes=0.5)])
ax.set_xticklabels(["t_0", "t_1   t_2", "t_3", ""])
sns.lineplot(x="time", y="value_2", data=cpu_util, label="Storm 2")
sns.lineplot(x="time", y="value_4", data=cpu_util, label="Storm 3")
sns.lineplot(x="time", y="value_3", data=cpu_util, label="Kafka")
sns.lineplot(x="time", y="value_5", data=cpu_util, label="Utilities")


pyplot.savefig("outputs/"+run + "/"+ run + "_metrics.pdf")
pyplot.show()


