from matplotlib.ticker import Formatter
import pandas as pd
import time
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
import seaborn as sns
import matplotlib.dates as mdates
from load_meta import *
from input import *
import sys
import os
import matplotlib.ticker as mtick
try: 
    os.mkdir("outputs/"+run+"/")
except FileExistsError:
    print(run + " Directory exists")
sys.stdout = open('outputs/' + run + '/latency_output.txt', 'w')

date_fmt = mdates.DateFormatter('%H:%M:%S')
path="/home/jfr/Thesis/kafka-logs/"

sns.set()
filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"
#fig, axes = pyplot.subplots( figsize = ( 10, 5))
df = pd.read_csv(filename, sep=";",parse_dates=True, infer_datetime_format=True)

latency = df[[" record.timestamp", "latencylocal", " partition", "note", " window_size"]]
latency[' record.timestamp'] = latency[' record.timestamp'].apply(lambda ts: pd.to_datetime(ts, unit='ms').to_pydatetime())
latency[' latency'] = latency['latencylocal']

#print("average latency:\n", latency.groupby([" partition", "note"]).mean())
#print(latency.groupby(["note"])[" latency"].mean())

# print("max latency:\n", latency.groupby([" partition", "note"])[" latency"].max())
# print(latency.groupby(["note"])[" latency"].max())

# print("min latency:\n", latency.groupby([" partition", "note"])[" latency"].min())
# print(latency.groupby(["note"])[" latency"].min())

# print("0.5 quantile latency:\n", latency.groupby([" partition", "note"])[" latency"].quantile(0.5))
# print(latency.groupby(["note"])[" latency"].quantile(0.5))

check = (latency.groupby(["note"]).get_group(" check"))
event_count_check = check[" window_size"].sum()

benchmark = (latency.groupby(["note"]).get_group(" benchmark"))
event_count_benchmark = benchmark[" window_size"].sum()

latency = check.append(benchmark)



####

end_benchmark = latency[latency.note == " benchmark"][" record.timestamp"].iloc[-1]
duration_benchmark = (end_benchmark-start_benchmark).seconds

end_check = latency[latency.note == " check"][" record.timestamp"].iloc[-1]
duration_check = (end_check - start_check).seconds

# rolling avg:
latency["rollingAVG"] = latency[" latency"].rolling(window=6).mean()

# grid figure with own graph for each partition
grid = sns.FacetGrid(data=latency, col=' partition', col_wrap=3)
grid.map(sns.lineplot, ' record.timestamp', ' latency')
for axis in grid.axes:
    axis.xaxis.set_major_formatter(date_fmt)
    #axis.legend(loc=5)
    axis.set_xticks([start_check, start_benchmark, fault_end, fault_end + timedelta(minutes=0.5)])
    axis.set_xticklabels(["t_0", "t_1   t_2", "t_3", ""])
    axis.axvline(x=fault_begin, color='r', linestyle=':')
    axis.axvline(x=fault_end, color='r', linestyle=':')
    axis.axvline(x=start_benchmark, color="g", linestyle = "-")
   # annotate()

latency_per_partition = latency.groupby(' partition')    
latency_partition=[latency_per_partition.get_group(x) for x in latency_per_partition.groups]
pyplot.savefig('outputs/' + run + "/" + run + "_split_latency.pdf")
fig, axes = pyplot.subplots( figsize = ( 10, 5))

#output for latex table
print("Partition;AVG_control;AVG_benchmark;MAX_control;MAX_benchmark;MIN_control;MIN_benchmark;MEDIAN_control;MEDIAN_Benchmark")
for partition in latency_partition:
    partition_num = str(partition.iloc[0][" partition"] + 1)
    partition_check=partition[partition.note==" check"]
    partition_benchmark=partition[partition.note==" benchmark"]
    print(partition_num
        , round(partition_check.mean()["latencylocal"], 2)
        , round(partition_benchmark.mean()["latencylocal"], 2)
        , partition_check.max()["latencylocal"]
        , partition_benchmark.max()["latencylocal"]
        , partition_check.min()["latencylocal"]
        , partition_benchmark.min()["latencylocal"]
        , partition_check.median()["latencylocal"]
        , partition_benchmark.median()["latencylocal"]
        , sep="&"
        ,end=""
        ,flush=True)
    print("\\\\ \\hline")
    sns.lineplot(x=" record.timestamp", y=" latency", data = partition, label = "partition " + partition_num, legend=False)
    #marker="o"

print("Total"
    ,round(latency.groupby(["note"])[" latency"].mean()[" check"], 2)
    ,round(latency.groupby(["note"])[" latency"].mean()[" benchmark"], 2)
    ,round(latency.groupby(["note"])[" latency"].max()[" check"], 2)
    ,round(latency.groupby(["note"])[" latency"].max()[" benchmark"], 2)
    ,round(latency.groupby(["note"])[" latency"].min()[" check"], 2)
    ,round(latency.groupby(["note"])[" latency"].min()[" benchmark"], 2)
    ,round(latency.groupby(["note"])[" latency"].median()[" check"], 2)
    ,round(latency.groupby(["note"])[" latency"].median()[" benchmark"], 2)
    ,sep = "&"
    ,end=""
    ,flush=True)
print("\\\\ \\hline")
#setting axes ticks etc.
axes.xaxis.set_major_formatter(date_fmt)
axes.legend(loc=5)
axes.set_xticks([start_check, start_benchmark, fault_end, fault_end + timedelta(minutes=0.5)])
axes.set_xticklabels(["t_0", "t_1   t_2", "t_3", ""])
annotate()
pyplot.savefig('outputs/' + run + "/" + run + "_latency.pdf")

# seperate rollling AVG plot
fig, axes = pyplot.subplots( figsize = ( 10, 5))
sns.lineplot(x =" record.timestamp", y="rollingAVG", data= latency, label = "Average")
axes.set_xticks([start_check, start_benchmark, fault_end, fault_end + timedelta(minutes=0.5)])
axes.set_xticklabels(["t_0", "t_1   t_2", "t_3", ""])
annotate()
pyplot.savefig('outputs/' + run + "/" + run + "_latency_rolling.pdf")

#average latency over partitions: 
#TODO:
avg_latency = latency.set_index(pd.DatetimeIndex(latency[" record.timestamp"])).sort_index()
avg_latency = avg_latency[[" record.timestamp", "latencylocal"]].resample("5s").mean()
print(avg_latency)
fig, axes = pyplot.subplots( figsize = ( 10, 5))
sns.lineplot(x =" record.timestamp", y="latencylocal", data=avg_latency)
axes.set_xticks([start_check, start_benchmark, fault_end, fault_end + timedelta(minutes=0.5)])
axes.set_xticklabels(["t_0", "t_1   t_2", "t_3", ""])
annotate()
pyplot.savefig('outputs/' + run + "/" + run + "_latency_average.pdf")

############ other metrics:

pattern = "%Y-%m-%d %H:%M:%S"
metric_path="/home/jfr/Thesis/prom-metrics/"
throughput_sum_file = metric_path + run + "/storm_topology_Windowbolt_Throughput_sum.csv"
throughput_file = metric_path + run + "/storm_topology_Windowbolt_Throughput.csv"
cpu_file = metric_path + run + "/node_cpu_seconds_total_1m.csv"
#################################################################

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
throughput_check = throughput[(throughput.time > start_check) & (throughput.time < start_benchmark)]
throughput_benchmark = throughput[throughput.time >= start_benchmark]
print("Throughput: ")
for col in throughput.columns[2:]:
    i += 1
    sns.lineplot(x="time", y=col, data=throughput[throughput.time > start_check], label = "Component " + str(i))
    partition_check = throughput_check[col][throughput_check[col] > 0]
    partition_benchmark = throughput_benchmark[col][throughput_benchmark[col] > 0]
    print(col
        ,partition_check.mean()
        ,partition_benchmark.mean()
        ,partition_check.max()
        ,partition_benchmark.max()
        ,partition_check.min()
        ,partition_benchmark.min()
        ,partition_check.median()
        ,partition_benchmark.median()
        ,sep = " & ")

throughput_melted = throughput.melt( id_vars = ["time", "id"] , var_name = "Component" , value_name = "Throughput")
throughput_check =  throughput_melted[(throughput_melted.time > start_check) & (throughput_melted.time < start_benchmark) & (throughput_melted.Throughput > 0)]
throughput_benchmark = throughput_melted[(throughput_melted.time >= start_benchmark) & (throughput_melted.Throughput > 0)]

print("Total"
    ,throughput_check.mean()["Throughput"]
    ,throughput_benchmark.mean()["Throughput"]
    ,throughput_check.max()["Throughput"]
    ,throughput_benchmark.max()["Throughput"]
    ,throughput_check.min()["Throughput"]
    ,throughput_benchmark.min()["Throughput"]
    ,throughput_check.median()["Throughput"]
    ,throughput_benchmark.median()["Throughput"]
    ,sep = " & "
    )

calc_throughput_check = event_count_check / duration_check
calc_throughput_benchmark = event_count_benchmark / duration_benchmark
print("Calculted total throughputs: \ncheck: ", calc_throughput_check,"benchmark: ", calc_throughput_benchmark, "Diff: ", round(((calc_throughput_benchmark - calc_throughput_check) / calc_throughput_check) * 100, 2), "%")

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
sns.lineplot(x="time", y="value_5", data=cpu_util, label="Storm 3")
sns.lineplot(x="time", y="value_3", data=cpu_util, label="Kafka")
sns.lineplot(x="time", y="value_4", data=cpu_util, label="Utilities")


pyplot.savefig("outputs/"+run + "/"+ run + "_metrics.pdf")

pyplot.show()


