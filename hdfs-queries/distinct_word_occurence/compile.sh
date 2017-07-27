#!/bin/bash

cd src
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
cd ..
