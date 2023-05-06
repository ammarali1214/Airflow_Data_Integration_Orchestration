-- SNOWFLAKE STORED PROCEDURES FOR DATA TRANSFORMATION

-- FOR D_CUSTOMER TABLE
CREATE OR REPLACE PROCEDURE insert_d_customer()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
var sql_command = "INSERT INTO D_CUSTOMER (customer_id, customer_name, customer_zip_cd) SELECT customer_id, customer_name, ZIP_CODE FROM AIRBYTE_DATABASE.AIRBYTE_SCHEMA.CUSTOMER";
var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Values inserted successfully!";
$$;


-- FOR D_PRODUCT TABLE
create or replace PROCEDURE insert_d_product()
returns VARCHAR
LANGUAGE javascript
as
$$
var sql_command = "insert into D_PRODUCT(PRODUCT_ID, CATEGORY_ID, PRODUCT_NAME) select PRODUCT_ID, CATEGORY_ID, PRODUCT_NAME from AIRBYTE_DATABASE.AIRBYTE_SCHEMA.PRODUCT";
var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Values inserted from AirByte";
$$;


-- FOR D_ PRODUCT_CATEGORY TABLE
create or replace PROCEDURE insert_d_product_category()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
var sql_command = "INSERT INTO D_PRODUCT_CATEGORY(CATEGORY_ID, CATEGORY_NAME) SELECT CATEGORY_ID, CATEGORY_NAME from AIRbYTE_DATABASE.AIRBYTE_SCHEMA.PROD_CATEGORY";
var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Values inserted from AirByte";
$$;


-- FOR D_CUSTOMER_GEOGRAPHY TABLE
Create or replace procedure insert_d_customer_geography()
Returns varchar
Language javascript
As
$$
var sql_command = “insert into d_customer_geography(zip_code, city, county, state_code, state_name) select ZIPCODE, CITY, COUNTY, STATE_ABBR, STATE from AIRBYTE_DATABASE.AIRBYTE_SCHEMA.CUSTOMER_GEOGRAPHY_DATA”;
var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Values inserted from AirByte";
$$;


-- FOR F_ORDERS TABLE
CREATE OR REPLACE PROCEDURE insert_F_ORDERS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
var sql_command = " INSERT INTO F_ORDERS(ORDER_ID, CUSTOMER_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY, DISCOUNT, TOTAL_PRICE, ORDER_DATE) SELECT O.ORDER_ID, C.CUSTOMER_ID, O.PRODUCT_ID, O.UNIT_PRICE, O.QUANTITY, O.DISCOUNT, O.TOTAL_PRICE, C.ORDER_DATE FROM AIRBYTE_DATABASE.AIRBYTE_SCHEMA.order_details O LEFT JOIN AIRBYTE_DATABASE.AIRBYTE_SCHEMA.customer_order C ON O.ORDER_ID = C.ORDER_ID";
var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Values inserted from AirByte";
$$;


-- FOR AGG_PRODUCT_PERFORMANCE
create or replace PROCEDURE insert_ agg_product_performance()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
var sql_command = "INSERT INTO agg_product_performance (product_id, product_category_id, state_cd, day_date,  total_orders, total_revnue, AVG_PROD_REVIEW_SCORE ) select f.product_id, p.category_id product_category_id, g.zip_code state_cd, f.order_date day_date, count(f.order_id) total_orders, sum(f.total_price) total_revnue,  avg(cr.PRODUCT_REVIEW_SCORE) AVG_PROD_REVIEW_SCORE from F_ORDERS f join d_product p on p.product_id = f.product_id join d_customer dc on dc.customer_id=f.customer_id join d_customer_geography g on dc.customer_zip_cd=g.zip_code join airbyte_database.airbyte_schema.customer_reviews_data cr on cr.product_id=f.product_id group by f.product_id, product_category_id, day_date, zip_code";
var stmt = snowflake.createStatement({sqlText: sql_command});
stmt.execute();
return "Values inserted from AirByte";
$$;
