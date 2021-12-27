import seaborn as sns
import pandas as pd
from matplotlib import pyplot
#import matplotlib.pyplot as plt
import numpy as np
from seaborn.palettes import color_palette
#plt.rcParams.update({'font.size': 40})

delta_mean_latency_tp_50 = [1.92, 9.03, 17.9, 27.7, 79.24]
delta_mean_latency_tp_100 = [5.15, 7.02, 22.03, 31.92, 491.51]
delta_mean_latency_tp_200 = [0.92, 8.99, 11.74, 157.6, None]
delta_mean_latency_tp_400 = [0.5, 5.22, 85.99, None, None]
delta_mean_latency_tp_600 = [-1.15, 8.6, 808.48, None, None]
# delta_mean_latency_tp_200 =
# delta_mean_latency_tp_400
# delta_mean_latency_tp_600
index = ["10ms", "20ms", "50ms", "100ms", "200ms"]
df = pd.DataFrame({'TP_50': delta_mean_latency_tp_50,
                   'TP_100': delta_mean_latency_tp_100,
                   'TP_200': delta_mean_latency_tp_200,
                   'TP_400': delta_mean_latency_tp_400,
                   'TP_600': delta_mean_latency_tp_600,
                   'throughput': index
                   },
                  index=index)
sns.set()
# sns.color_palette()
sns.color_palette("icefire", as_cmap=True)
# # fig, ax = pyplot.subplots(figsize = ( 15, 5))
ax = df.plot(figsize=(22, 15), kind="barh", width=0.9)  # , colormap="BuPu")
pyplot.xscale("symlog")
pyplot.xlim(left=-2, right=1500)
pyplot.xticks(fontsize=25)
pyplot.yticks(fontsize=25)
pyplot.ylabel("Delay [ms]", fontsize=28)
pyplot.xlabel("\u0394 mean event-time latency %", fontsize=28)
pyplot.legend(fontsize=25, loc=2)
# For each bar: Place a label
for rect in ax.patches:
    # Get X and Y placement of label from rect.
    x_value = rect.get_width()
    y_value = rect.get_y() + rect.get_height() / 2

    # Number of points between bar and label. Change to your liking.
    space = 2
    # Vertical alignment for positive values
    ha = 'left'

    if x_value == 0:
        label = ""  # "Disregarded"
    else:
        # If value of bar is negative: Place label left of bar
        if x_value < 0:
            # Invert space to place label to the left
            space *= -1
            # Horizontally align label at right
            ha = 'right'

        # Use X value as label and format number with one decimal place
        label = "{:.1f}%".format(x_value)

    # Create annotation
    pyplot.annotate(
        label,                      # Use `label` as label
        (x_value, y_value),         # Place label at end of the bar
        xytext=(space, 0),          # Horizontally shift label by `space`
        textcoords="offset points",  # Interpret `xytext` as offset in points
        va='center',                # Vertically center label
        ha=ha,
        fontsize=28)                      # Horizontally align label differently for
    # positive and negative values.
pyplot.savefig("barplot.pdf", bbox_inches='tight')
pyplot.show()
