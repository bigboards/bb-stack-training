#!/bin/bash
set -e
MYSQL_HOST=${1:-"192.168.2.184"}

echo "Importing the weather.stations table to /data/raw/weather/stations"
# ...

echo "Importing the weather.states table to /data/raw/weather/states"
# ...

echo "Importing the weather.countries table to /data/raw/weather/countries"
# ...

echo "Importing the weather.facts_2006 table to /data/raw/weather/facts/2006"
# ...