#!/bin/bash
set -e
MYSQL_HOST=${1:-"192.168.2.184"}

echo "Importing the retail.stores table to /data/raw/retail/stores"
# ...

echo "Importing the retail.customers table to /data/raw/retail/customers"
# ...

echo "Importing the retail.employees table to /data/raw/retail/employees"
# ...

echo "Importing the retail.products table to /data/raw/retail/products"
# ...

echo "Importing the retail.sales table to /data/raw/retail/sales"
# ... Make sure to use -m 100