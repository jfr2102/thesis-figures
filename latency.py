from matplotlib.ticker import Formatter
import pandas as pd
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
import seaborn as sns
import matplotlib.dates as mdates
from load_meta import *
from input import *
date_fmt = mdates.DateFormatter('%H:%M:%S')
path="/home/jfr/Thesis/kafka-logs/"

# input: ##########################
#run="2021-11-15_22-32-10_delay_200"
#fault_begin = "2021-11-10-17:40:46"
#failure_duration_minutes = 7
#failure_duration_seconds = 0
###################################

filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"
fig, axes = pyplot.subplots( figsize = ( 10, 5))
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
    sns.lineplot(x=" record.timestamp", y=" latency", data = partition, label = "partition " + partition_num, legend=False)
    #marker="o"
    
# plot using rolling average
#sns.lineplot( x =" record.timestamp",
#             y = 'rolling_avg_10',
#             data = latency_partition[0],
#             label = 'Rollingavg')
#pyplot.ylim(bottom=500)

axes.xaxis.set_major_formatter(date_fmt)
axes.legend(loc=5)
annotate()
#min, ymax = pyplot.ylim()
#arrowprops = {'width': 1, 'headwidth': 1, 'headlength': 1, 'shrink':0.05 }
#pyplot.annotate('BigNews1', xy=(benchmark_begin_loaded, ymax))
#pyplot.tight_layout()
trans = axes.get_xaxis_transform()
# pyplot.text(start_benchmark, 0.95, 't_0', transform=trans, ha="right", color="g")
# pyplot.text(fault_begin, 0.95, 't_1', transform=trans, ha="right", color = "r")
# pyplot.text(fault_end, 0.95, 't_2', transform=trans, ha="right", color = "r")

pyplot.savefig(run + ".pdf")
#sns.lineplot(x=" record.timestamp", y=" latency", data = latency, hue= " partition", marker="o")
pyplot.show()

