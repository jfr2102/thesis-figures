import pandas as pd
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
from input import *

# def load_meta(run):
filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"

# todo: load benchmark and check_begin from produceroutput.csv file?
producerlog = pd.read_csv(producerfilename, sep=";")
benchmark_begin_loaded = (producerlog.loc[lambda df: df['note'] == "benchmark",:])['timestamp'].iloc[0]
check_begin_loaded = (producerlog.loc[lambda df: df['note'] == "check",:])['timestamp'].iloc[0]

testdriverinfo = pd.read_csv(testdriverfilename, sep=";")
fault_begin_loaded = testdriverinfo["failure_start"].iloc[0]
fault_end_loaded = testdriverinfo["failure_end"].iloc[0]

#calc 
timepattern = "%Y-%m-%d-%H-%M-%S"
fault_begin = datetime.strptime(fault_begin_loaded, timepattern) #- timedelta(hours = 1)
fault_end = datetime.strptime(fault_end_loaded, timepattern) #- timedelta(hours = 1)
#fault_begin = datetime.strptime(fault_begin, "%Y-%m-%d-%H:%M:%S")- timedelta(hours=1)
#fault_end =  fault_begin + timedelta(minutes = failure_duration_minutes, seconds=failure_duration_seconds)
start_benchmark = pd.to_datetime(benchmark_begin_loaded, unit='ms').to_pydatetime()
start_check = pd.to_datetime(check_begin_loaded, unit='ms').to_pydatetime()
print(start_benchmark, fault_begin, fault_end)
def annotate():
    pyplot.axvline(x=fault_begin, color='r', linestyle=':')
    pyplot.axvline(x=fault_end, color='r', linestyle=':')
    pyplot.axvline(x=start_benchmark, color="g", linestyle = "-")
