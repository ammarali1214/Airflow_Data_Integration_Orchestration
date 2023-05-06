# 1. Import modules
from airflow import DAG
from airflow.operators.empty import EmptyOperator 
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator 
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import pendulum
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


    
# 2. Define default arguments
default_args = {
    'owner': 'Ammar', 
    'depends_on_past': False
     }

# 3. Instantiate the DAG
dag = DAG(
    dag_id = 'Airflow_Airbyte_Snowflake_ETL', 
    default_args = default_args,
    description = 'Data Integration and Orchestration (Airbyte, Airflow, Snowflake)',
    start_date= pendulum.today('UTC').subtract(days=1), # this DAG will be executed immediately as its set to start on previous day as of today
    schedule = '@daily'
    )


# 4.Define tasks

# start_task is defined to make sure if DAG is started successfully
start_task = EmptyOperator(
        task_id = 'start',
        dag = dag
        )

# task_mysql_sf task is defined to trigger Airbyte Connection of MySQL <--> Snowflake to extract and load Data from MySQL to Snowflake
task_mysql_sf = AirbyteTriggerSyncOperator(
        task_id = 'mysql_snowflake_sync', 
        airbyte_conn_id = 'airflow_airbyte', 
        connection_id = '7f948beb-2a0f-4532-97fc-3a80ab65e589',
        asynchronous = True,
        dag = dag
        )                                                       

# mysql_sensor task is defined to validate if Data Syncing is successfully completed or not
mysql_sensor = AirbyteJobSensor(
        task_id='airbyte_sensor_mysql_sf', 
        airbyte_conn_id='airflow_airbyte', 
        airbyte_job_id= task_mysql_sf.output,
        dag = dag
        )
        

# task_API1_sf task is defined to trigger Airbyte Connection of File <--> Snowflake to extract and load Data from REST API to Snowflake  
task_API1_sf = AirbyteTriggerSyncOperator(
        task_id = 'Online_Shop_API1', 
        airbyte_conn_id = 'airflow_airbyte', 
        connection_id = 'da283507-c066-4d19-b8b8-21ebf66601a4',
        asynchronous = True,
        dag = dag
        )

# API1_sensor task is defined to validate if Data Syncing is successfully completed or not
API1_sensor = AirbyteJobSensor(
        task_id='airbyte_sensor_API1',
        airbyte_conn_id='airflow_airbyte',
        airbyte_job_id=task_API1_sf.output,
        dag = dag
        )


# task_API2_sf task is defined to trigger Airbyte Connection of File <--> Snowflake to extract and load Data from REST API to Snowflake
task_API2_sf = AirbyteTriggerSyncOperator(
        task_id = 'Geography_API2', 
        airbyte_conn_id = 'airflow_airbyte', 
        connection_id = '724ab7ff-e841-4350-93b4-cbe3927d1bc9', 
        asynchronous = True,
        dag = dag
        )

# API2_sensor task is defined to validate if Data Syncing is successfully completed or not
API2_sensor = AirbyteJobSensor(
        task_id='airbyte_sensor_API2', 
        airbyte_conn_id='airflow_airbyte', 
        airbyte_job_id= task_API2_sf.output,
        dag = dag
        )

# sf_task1 task is defined to execute the Snowflake Task for data transformation on Snowflake
sf_task1= SnowflakeOperator(
        task_id='Snowflake_d_prod_category',
        sql='CALL TASK TASK_D_PRODUCT_CATEGORY',
        snowflake_conn_id='snowflake',
        dag = dag
        )

# sf_task2 task is defined to execute the Snowflake Task for data transformation on Snowflake
sf_task2= SnowflakeOperator(
        task_id='Snowflake_d_product',
        sql='CALL TASK TASK_D_PRODUCT',
        snowflake_conn_id='snowflake',
        dag = dag
        )

# sf_task3 task is defined to execute the Snowflake Task for data transformation on Snowflake
sf_task3 = SnowflakeOperator(
        task_id='Snowflake_d_customer_geography',
        sql='CALL TASK TASK_D_CUSTOMER_GEOGRAPHY',
        snowflake_conn_id='snowflake',
        dag = dag
        )

# sf_task4 task is defined to execute the Snowflake Task for data transformation on Snowflake
sf_task4 = SnowflakeOperator(
        task_id='Snowflake_d_customer',
        sql='CALL TASK TASK_D_CUSTOMER',
        snowflake_conn_id='snowflake',
        dag = dag
        )

# sf_task5 task is defined to execute the Snowflake Task for data transformation on Snowflake
sf_task5 = SnowflakeOperator(
        task_id='Snowflake_f_order',
        sql='CALL TASK TASK_F_ORDERS',
        snowflake_conn_id='snowflake',
        dag = dag
        )

# sf_task6 task is defined to execute the Snowflake Task for data transformation on Snowflake
sf_task6 = SnowflakeOperator(
        task_id='Snowflake_AGG_PRODUCT_PERFORMANCE',
        sql='CALL TASK TASK_AGG_PRODUCT_PERFORMANCE',
        snowflake_conn_id='snowflake',
        dag = dag
        )

# end_task is defined to make sure if DAG has ended successfully
end_task = EmptyOperator(task_id = 'end',
                         dag = dag)


# 5.Define dependencies

start_task >> [task_mysql_sf, task_API1_sf, task_API2_sf] >> mysql_sensor >> API1_sensor>> API2_sensor >> sf_task1>> sf_task2>> sf_task3>> sf_task4>> sf_task5>> sf_task6>> end_task
