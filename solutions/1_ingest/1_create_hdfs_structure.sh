#!/bin/bash
echo "Creating the directory structure on HDFS"
hadoop fs -mkdir /data
hadoop fs -mkdir /data/raw
hadoop fs -mkdir /data/raw/weather
hadoop fs -mkdir /data/raw/retail