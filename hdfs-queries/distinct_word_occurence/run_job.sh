#!/bin/bash

hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
hadoop jar wc.jar WordCount /datasets/aol /map_reduce_output
