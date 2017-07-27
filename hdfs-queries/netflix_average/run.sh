#!/bin/bash

cd src
hdfs dfs -rm -r /mr_tmp
hdfs dfs -rm -r /mr_out
hadoop jar wc.jar WordCount /datasets/netflix/large /mr_tmp /mr_out
cd ..
rm -rf out/*
hdfs dfs -get /mr_out out/
