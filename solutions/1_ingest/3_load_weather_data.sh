#!/bin/bash
set -e
MYSQL_HOST=${1:-"192.168.2.184"}

echo "Importing the weather database"
SQOOP_IMP_WEATHER="sqoop import --connect jdbc:mysql://${MYSQL_HOST}/weather --username root --password root"
${SQOOP_IMP_WEATHER} --table stations --target-dir /data/raw/weather/stations
${SQOOP_IMP_WEATHER} --table states --target-dir /data/raw/weather/states
${SQOOP_IMP_WEATHER} --table countries --target-dir /data/raw/weather/countries
${SQOOP_IMP_WEATHER} --table facts_2006 --target-dir /data/raw/weather/facts/2006 --split-by station_id