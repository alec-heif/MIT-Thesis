#!/bin/bash

cd src
hdfs dfs -rm -r /mr_tmp
hdfs dfs -rm -r /mr_out
hadoop jar wc.jar WordCount /datasets/aol /mr_tmp /mr_out
cd ..
