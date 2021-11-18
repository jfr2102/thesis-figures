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
try: 
    os.mkdir("outputs/"+run+"/")
except FileExistsError:
    print(run + " Directory exists")
sys.stdout = open('outputs/' + run + '/output.txt', 'w')

date_fmt = mdates.DateFormatter('%H:%M:%S')
path="/home/jfr/Thesis/kafka-logs/"

sns.set()
filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"
fig, axes = pyplot.subplots( figsize = ( 10, 5))
df = pd.read_csv(filename, sep=";",parse_dates=True, infer_datetime_format=True)

latency = df[[" record.timestamp", "latencylocal", " partition", "note"]]
latency[' record.timestamp'] = latency[' record.timestamp'].apply(lambda ts: pd.to_datetime(ts, unit='ms').to_pydatetime())
latency[' latency'] = latency['latencylocal']

print("average latency:\n", latency.groupby([" partition", "note"]).mean())
print(latency.groupby(["note"])[" latency"].mean())

print("max latency:\n", latency.groupby([" partition", "note"])[" latency"].max())
print(latency.groupby(["note"])[" latency"].max())

print("min latency:\n", latency.groupby([" partition", "note"])[" latency"].min())
print(latency.groupby(["note"])[" latency"].min())

print("0.5 quantile latency:\n", latency.groupby([" partition", "note"])[" latency"].quantile(0.5))
print(latency.groupby(["note"])[" latency"].quantile(0.5))

check = benchmark = (latency.groupby(["note"]).get_group(" check"))
benchmark = (latency.groupby(["note"]).get_group(" benchmark"))
latency = check.append(benchmark)

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
axes.set_xticks([start_check, start_benchmark, fault_end, fault_end + timedelta(minutes=0.5)])
axes.set_xticklabels(["t_0", "t_1   t_2", "t_3", ""])
annotate()
#min, ymax = pyplot.ylim()
#arrowprops = {'width': 1, 'headwidth': 1, 'headlength': 1, 'shrink':0.05 }
#pyplot.annotate('BigNews1', xy=(benchmark_begin_loaded, ymax))
#pyplot.tight_layout()
trans = axes.get_xaxis_transform()
# pyplot.text(start_benchmark, 0.95, 't_0', transform=trans, ha="right", color="g")
# pyplot.text(fault_begin, 0.95, 't_1', transform=trans, ha="right", color = "r")
# pyplot.text(fault_end, 0.95, 't_2', transform=trans, ha="right", color = "r")

pyplot.savefig('outputs/' + run + "/" + run + "_latency.pdf")
#sns.lineplot(x=" record.timestamp", y=" latency", data = latency, hue= " partition", marker="o")
pyplot.show()
import metrics
