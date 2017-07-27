#!/bin/bash

cd src
hdfs dfs -rm -r /mr_tmp_1
hdfs dfs -rm -r /mr_out_1
hdfs dfs -rm -r /mr_tmp_2
hdfs dfs -rm -r /mr_out_2
hdfs dfs -rm -r /mr_tmp_3
hdfs dfs -rm -r /mr_out_3
hdfs dfs -rm -r /mr_tmp_4
hdfs dfs -rm -r /mr_out_4
hadoop jar wc.jar WordCount /datasets/aol /mr_tmp /mr_out
cd ..
rm -rf out/*
mkdir -p out
hdfs dfs -get /mr_out_1 out/mr_out_1
hdfs dfs -get /mr_out_2 out/mr_out_2
hdfs dfs -get /mr_out_3 out/mr_out_3
hdfs dfs -get /mr_out_4 out/mr_out_4
