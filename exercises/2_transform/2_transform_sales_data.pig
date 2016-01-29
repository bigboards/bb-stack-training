/** *************************************************************************************************************************
 ** transform_retail_data.pig : Transform the retail data into one big record to store in our master store.
 **
 **    We will gradually build the full records, starting with the sales data and proceeding through all
 **    dimension tables.
 ** ********************************************************************************************************************** */
sales =
    LOAD '/data/raw/retail/sales'
    USING PigStorage(',')
    AS (
        product_key:int,
        product_version:int,
        store_key:int,
        promotion_key:int,
        customer_key:int,
        employee_key:int,
        pos_transaction_number: int,
        qty: int,
        price: int,
        transaction_type: chararray,
        tender_type: chararray,
        date: datetime
    );
    
/*
 * Link the stores to the sales.
 *  Make sure the stores relation only contains tuples with store_key, store_name and store_state.
 *  After that, join sales and stores by their store_key and store it in relation linked_1
 */
raw_stores =
    LOAD '/data/raw/retail/stores'
    USING PigStorage(',')
    AS (
        store_key: int,
        store_name: chararray,
        store_state: chararray,
        store_region: chararray
    );
    
stores =
linked_1 =
         
/*
 * Link the employees to the sales.
 *  Make sure the employees relation contains the following fields:
 *    employee_key
 *    employee_name which is a concatination of their first and last name
 *    employee_gender
 *    employee_state
 *    employee_job_title
 *
 *  After that, link the employees with the previous linked_1 relation.
 */
raw_employees =
    LOAD '/data/raw/retail/employees'
    USING PigStorage(',')
    AS (
        employee_key: int,
        employee_gender: chararray,
        employee_first_name: chararray,
        employee_last_name: chararray,
        employee_state: chararray,
        employee_job_title: chararray
    );
    
employees =
linked_2 =
         
/*
 * Link the products to the sales.
 *  Link the previous created relation linked_2 to the products. Use the product_key as well as the
 *  product_version to link by.
 */
products =
    LOAD '/data/raw/retail/products'
    USING PigStorage(',')
    AS (
        product_key: int,
        product_version: int,
        product_description: chararray,
        product_category: chararray,
        product_department: chararray,
        product_price: int
    );
        
linked_3 =
         
/*
 * Link the customers to the sales.
 *  Link the previous created relation linked_3 to the customers.
 */
customers =
    LOAD '/data/raw/retail/customers'
    USING PigStorage(',')
    AS (
        customer_key: int,
        customer_gender: chararray,
        customer_age: int,
        customer_marital_status: chararray,
        customer_name: chararray,
        customer_state: chararray
    );

linked_4 =
         
/*
    Now we will have to clean our linked record.
*/
sales =
    FOREACH linked_4
    GENERATE
        sales::pos_transaction_number AS transaction_number,
        sales::qty AS qty,
        sales::price AS price,
        sales::transaction_type AS transaction_type,
        sales::tender_type AS tender_type,
        sales::date AS date,
        stores::store_key AS store_key,
        stores::store_name AS store_name,
        stores::store_state AS store_state,
        employees::employee_key AS employee_key,
        employees::employee_name AS employee_name,
        employees::employee_gender AS employee_gender,
        employees::employee_state AS employee_state,
        employees::employee_job_title AS employee_job_title,
        products::product_key AS product_key,
        products::product_version AS product_version,
        products::product_description AS product_description,
        products::product_category AS product_category,
        products::product_department AS product_department,
        products::product_price AS product_price,
        customers::customer_key AS customer_key,
        customers::customer_gender AS customer_gender,
        customers::customer_age AS customer_age,
        customers::customer_marital_status AS customer_marital_status,
        customers::customer_name AS customer_name,
        customers::customer_state AS customer_state;

STORE sales
    INTO '/data/master/sales'
    USING PigStorage();
