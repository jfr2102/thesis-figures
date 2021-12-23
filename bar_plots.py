import seaborn as sns
import pandas as pd
from matplotlib import pyplot
delta_mean_latency_tp_50 = [1.92, 9.03, 17.9, 27.7, 79.24]
delta_mean_latency_tp_100 = [5.15, 7.02, 22.03, 31.92, 491.51]
delta_mean_latency_tp_200 = [5.15, 7.02, 22.03, 31.92, 491.51]
delta_mean_latency_tp_400 = [5.15, 7.02, 22.03, 31.92, 491.51]
delta_mean_latency_tp_600 = [5.15, 7.02, 22.03, 31.92, 491.51]
# delta_mean_latency_tp_200 =
# delta_mean_latency_tp_400
# delta_mean_latency_tp_600
sns.set()
index = ["10ms", "20ms", "50ms", "100ms", "200ms"]

df = pd.DataFrame({'TP_50': delta_mean_latency_tp_50,
                   'TP_100': delta_mean_latency_tp_100,
                   'TP_200': delta_mean_latency_tp_200,
                   'TP_400': delta_mean_latency_tp_400,
                   'TP_600': delta_mean_latency_tp_600,
                   'throughput': index
                   },
                  index=index)

ax = df.plot.bar(rot=0)
pyplot.xlabel("Delay [ms]")
pyplot.ylabel("\u0394 mean event-time latency %")
pyplot.savefig("barplot.pdf",  bbox_inches='tight')
pyplot.show()