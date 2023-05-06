--TASKS ONLINE_STORE_DWH

-- TASK_D_PRODUCT_CATEGORY
create or replace task TASK_D_PRODUCT_CATEGORY
warehouse = compute_wh
as call insert_d_product_category();           ## call <'Stored_Procedure_Name'>(); this statement is used to call/Initialize a snowflake stored Procedure

-- TASK_ D_PRODUCT
create or replace task TASK_D_PRODUCT
warehouse = COMPUTE_WH
as call insert_d_product();                   ## call <'Stored_Procedure_Name'>(); this statement is used to call/Initialize a snowflake stored Procedure

-- TASK_D_CUSTOMER
create or replace task TASK_D_CUSTOMER
warehouse = COMPUTE_WH
as call insert_d_customer();                 ## call <'Stored_Procedure_Name'>(); this statement is used to call/Initialize a snowflake stored Procedure

-- TASK_F_ORDERS
create or replace task TASK_F_ORDERS
WAREHOUSE = COMPUTE_WH
as call insert_f_orders();                    ## call <'Stored_Procedure_Name'>(); this statement is used to call/Initialize a snowflake stored Procedure


-- Task_agg_product_performance
create or replace task TASK_AGG_PRODUCT_PERFORMANCE
WAREHOUSE = COMPUTE_WH
as call insert_agg_product_performance();    ## call <'Stored_Procedure_Name'>(); this statement is used to call/Initialize a snowflake stored Procedure
 
