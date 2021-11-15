from matplotlib.ticker import Formatter
import pandas as pd
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
import seaborn as sns
import matplotlib.dates as mdates
date_fmt = mdates.DateFormatter('%H:%M:%S')
path="/home/jfr/Thesis/kafka-logs/"

# input: ##########################
run="2021-11-11_11-52-11_delay_20"
#fault_begin = "2021-11-10-17:40:46"
#failure_duration_minutes = 7
#failure_duration_seconds = 0
###################################

filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"
fig, axes = pyplot.subplots( figsize = ( 15, 5))
df = pd.read_csv(filename, sep=";",parse_dates=True, infer_datetime_format=True)

latency = df[[" record.timestamp", " latency", " partition", "note"]]
latency[' record.timestamp'] = latency[' record.timestamp'].apply(lambda ts: pd.to_datetime(ts, unit='ms').to_pydatetime())

print("average latency:\n", latency.groupby([" partition", "note"]).mean())
print(latency.groupby(["note"])[" latency"].mean())

print("max latency:\n", latency.groupby([" partition", "note"])[" latency"].max())
print(latency.groupby(["note"])[" latency"].max())

print("min latency:\n", latency.groupby([" partition", "note"])[" latency"].min())
print(latency.groupby(["note"])[" latency"].min())

print("0.5 quantile latency:\n", latency.groupby([" partition", "note"])[" latency"].quantile(0.5))
print(latency.groupby(["note"])[" latency"].quantile(0.5))

# rolling avg and plotting
# nach grouping machen?
#latency[ 'rolling_avg_10' ] = latency[' latency'].rolling(12).mean()
check = benchmark = (latency.groupby(["note"]).get_group(" check"))
benchmark = (latency.groupby(["note"]).get_group(" benchmark"))
latency = check.append(benchmark)
print(latency)
latency_per_partition = latency.groupby(' partition')    
latency_partition=[latency_per_partition.get_group(x) for x in latency_per_partition.groups]

#latency.plot(x=" record.timestamp", y=" latency")
for partition in latency_partition:
    partition_num = str(partition.iloc[0][" partition"] + 1)
    #print(partition)
    sns.lineplot(x=" record.timestamp", y=" latency", data = partition, label = "partition " + partition_num, legend=False, marker="o")
    #marker="o"
    
# plot using rolling average
#sns.lineplot( x =" record.timestamp",
#             y = 'rolling_avg_10',
#             data = latency_partition[0],
#             label = 'Rollingavg')
#pyplot.ylim(bottom=500)

axes.xaxis.set_major_formatter(date_fmt)
axes.legend(loc=5)
#input: 
# todo: load benchmark and check_begin from produceroutput.csv file?
producerlog = pd.read_csv(producerfilename, sep=";")
benchmark_begin_loaded = (producerlog.loc[lambda df: df['note'] == "benchmark",:])['timestamp'].iloc[0]

#@TODO
testdriverinfo = pd.read_csv(testdriverfilename, sep=";")
fault_begin_loaded = testdriverinfo["failure_start"].iloc[0]
fault_end_loaded = testdriverinfo["failure_end"].iloc[0]

#calc 
timepattern = "%Y-%m-%d-%H-%M-%S"
fault_begin = datetime.strptime(fault_begin_loaded, timepattern) - timedelta(hours = 1)
fault_end = datetime.strptime(fault_end_loaded, timepattern) - timedelta(hours = 1)
#fault_begin = datetime.strptime(fault_begin, "%Y-%m-%d-%H:%M:%S")- timedelta(hours=1)
#fault_end =  fault_begin + timedelta(minutes = failure_duration_minutes, seconds=failure_duration_seconds)
start_benchmark = pd.to_datetime(benchmark_begin_loaded, unit='ms').to_pydatetime()



#annotate
pyplot.axvline(x=fault_begin, color='r', linestyle=':')
pyplot.axvline(x=fault_end, color='r', linestyle=':')
pyplot.axvline(x=start_benchmark, color="g", linestyle = "-")

#min, ymax = pyplot.ylim()
#arrowprops = {'width': 1, 'headwidth': 1, 'headlength': 1, 'shrink':0.05 }
#pyplot.annotate('BigNews1', xy=(benchmark_begin_loaded, ymax))
#pyplot.tight_layout()
trans = axes.get_xaxis_transform()
#pyplot.text(start_benchmark, 0.95, 'Failure benchmark begin', transform=trans, ha="center", color="g")
#pyplot.text(fault_begin, 0.95, 'Failure Inection', transform=trans, ha="center", color = "r")
#pyplot.text(fault_end, 0.95, 'Failure End', transform=trans, ha="center", color = "r")

pyplot.savefig(run + ".pdf")
#sns.lineplot(x=" record.timestamp", y=" latency", data = latency, hue= " partition", marker="o")
pyplot.show()