#!/bin/bash
set -e
MYSQL_HOST=${1:-"192.168.2.184"}

echo "Importing the retail database"
SQOOP_IMP_RETAIL="sqoop import --connect jdbc:mysql://${MYSQL_HOST}/retail --username root --password root"
${SQOOP_IMP_RETAIL} --table stores --target-dir /data/raw/retail/stores
${SQOOP_IMP_RETAIL} --table customers --target-dir /data/raw/retail/customers
${SQOOP_IMP_RETAIL} --table employees --target-dir /data/raw/retail/employees
${SQOOP_IMP_RETAIL} --table products --target-dir /data/raw/retail/products
${SQOOP_IMP_RETAIL} --table sales --target-dir /data/raw/retail/sales --split-by store_key -m 100