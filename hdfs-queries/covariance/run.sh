#!/bin/bash

cd src
hdfs dfs -rm -r /mr_out

hadoop jar wc.jar WordCount /datasets/netflix/large /mr_out
cd ..
rm -rf out/*
mkdir -p out

hdfs dfs -get /mr_out out/mr_out
