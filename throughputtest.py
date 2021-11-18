from types import LambdaType
from matplotlib.ticker import Formatter
import matplotlib.ticker as mtick
from numpy import NaN
import pandas as pd
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
import seaborn as sns
import matplotlib.dates as mdates
from input import *
sns.set()
date_fmt = mdates.DateFormatter('%H:%M:%S')

filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"

fig, axes = pyplot.subplots( figsize = ( 10, 5))
df = pd.read_csv(filename, sep=";",parse_dates=True, infer_datetime_format=True)

latencies = df[[" record.timestamp", "latencylocal", " partition", "note"]]
latencies[' record.timestamp'] = latencies[' record.timestamp'].apply(lambda ts: pd.to_datetime(ts, unit='ms').to_pydatetime())
print(latencies)
ax = sns.lineplot(x=" record.timestamp", y="latencylocal", data =latencies[( ~ pd.isnull(latencies.note))], hue = " partition")
ax.set_ylabel("Latency [ms]")
ax.get_xaxis().set_major_formatter(date_fmt)
pyplot.savefig(run + "throughput_test.pdf")
pyplot.show()