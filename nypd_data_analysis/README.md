NYPD Data Analysis
===================

cd hadoop_record/example/
hadoop fs -put nypd.csv .
hadoop jar $HADOOP_HOME/hadoop-streaming.jar -input nypd.csv -output sample_output -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py -file nypd.py -file hadoop_record.mod
hadoop fs -cat sample_output/*

hadoop jar $HADOOP_HOME/hadoop-streaming.jar -input /data/data_in_jute_format/part-0* -inputformat SequenceFileAsTextInputFormat -output output_dir -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py -file nypd.py -file JuterecordClasses.jar -file hadoop_record.mod

With mapper.py, reducer.py, and yahoo.py in directory



///csv:

from hadoop_record import csv
