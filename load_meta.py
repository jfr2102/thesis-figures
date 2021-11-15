import pandas as pd
from datetime import datetime
from datetime import timedelta

path="/home/jfr/Thesis/kafka-logs/"
run="2021-11-15_19-05-12_delay_50"
filename= path + run + "/resultsoutput.csv"
producerfilename = path + run + "/produceroutput.csv"
testdriverfilename = path + run + "/testdriverinfo.txt"

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