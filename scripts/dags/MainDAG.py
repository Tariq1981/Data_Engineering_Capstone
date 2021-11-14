from datetime import datetime, timedelta
import configparser
import json

from airflow.operators.sql import SQLCheckOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import SQLQueries
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def getConfig(S3_config_con_id,key_name,bucket_name):
    hook = S3Hook(S3_config_con_id)
    file_contents = hook.read_key(key = key_name,bucket_name=bucket_name)
    config = configparser.ConfigParser()
    config.read_string(file_contents)
    jstr = json.dumps(config._sections)
    return jstr

def getConfigValue(jstr,Key1,Key2):
    dictConf = json.loads(jstr)
    return dictConf[Key1][Key2.lower()]


default_args = {
    'owner': 'Tariq',
    'start_date': datetime(2021, 11, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
    'email_on_retry': False
}
dag = DAG('Google_Apps_Analysis',
          default_args=default_args,
          description='Load and transform data in S3 and Redshift',
          schedule_interval=None,
          catchup=False,
          user_defined_macros={
              'getConfigValue':getConfigValue
          }
        )


JOB_FLOW_OVERRIDES = {
    "Name": "Google App Analyzer",
    "ReleaseLabel": "emr-5.33.1",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "Instances": {
        "InstanceGroups":
        [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.large",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster

    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


SPARK_FIRST_STEP = [
    {
        'Name': 'InstallBoto3',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'pip3', 'install', 'boto3'
            ]

        }
    },
    {
        'Name': 'LoadFirstLayerS3',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit','--master','yarn',
                '--deploy-mode','client',
                '--py-files',
                's3://capstone-tariq/Utitlity.py',
                '--files',
                's3://capstone-tariq/etl.cfg',
                's3://capstone-tariq/LoadFromS3ToS3.py'
            ]
        }
    }
]

SPARK_SECOND_STEP = [
    {
        'Name': 'LoadSecondLayerS3',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit','--master','yarn',
                '--deploy-mode','client',
                '--py-files',
                's3://capstone-tariq/Utitlity.py',
                '--files',
                's3://capstone-tariq/etl.cfg',
                's3://capstone-tariq/LoadAggLayer.py'
            ]
        }
    }
]
copy_options=['FORMAT AS PARQUET','COMPUPDATE OFF','STATUPDATE OFF']

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

get_config = PythonOperator(
    task_id="config_retrieval",
    python_callable=getConfig,
    op_kwargs={"S3_config_con_id":"aws_default",
               "key_name":Variable.get("ETL_Config_File"),
               "bucket_name":Variable.get("ETL_Config_Bucket")},
    dag=dag
)


create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id="postgres_default",
    sql=SQLQueries.create_schema.format(
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}"
    ),
    dag=dag
)
create_purge_app_fact = PostgresOperator(
    task_id='create_purge_app_fact_tbl',
    postgres_conn_id="postgres_default",
    sql=[
        SQLQueries.create_app_fact_table.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_FACT_FT')}}"
        ),
        SQLQueries.delete_table_sql.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_FACT_FT')}}"
        )
    ],
    dag=dag
)
create_purge_app_category = PostgresOperator(
    task_id='create_purge_app_category_tbl',
    postgres_conn_id="postgres_default",
    sql=[
        SQLQueries.create_app_category_table.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_CATEGORY_DM')}}"
        ),
        SQLQueries.delete_table_sql.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_CATEGORY_DM')}}"
        )
    ],
    dag=dag
)
create_purge_currency_type = PostgresOperator(
    task_id='create_purge_currency_type_tbl',
    postgres_conn_id="postgres_default",
    sql=[
        SQLQueries.create_currency_type_table.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CURRENCY_TYPE_DM')}}"
        ),
        SQLQueries.delete_table_sql.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CURRENCY_TYPE_DM')}}"
        )
    ],
    dag=dag
)
create_purge_developer = PostgresOperator(
    task_id='create_purge_developer_tbl',
    postgres_conn_id="postgres_default",
    sql=[
        SQLQueries.create_developer_table.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','DEVELOPER_DM')}}"
        ),
        SQLQueries.delete_table_sql.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','DEVELOPER_DM')}}"
        )
    ],
    dag=dag
)
create_purge_content_rating = PostgresOperator(
    task_id='create_purge_content_rating_tbl',
    postgres_conn_id="postgres_default",
    sql=[
        SQLQueries.create_content_rating_table.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CONTENT_RATING_DM')}}"
        ),
        SQLQueries.delete_table_sql.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CONTENT_RATING_DM')}}"
        )
    ],
    dag=dag
)
create_purge_permission_type = PostgresOperator(
    task_id='create_purge_permission_type_tbl',
    postgres_conn_id="postgres_default",
    sql=[
        SQLQueries.create_permission_type_table.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','PERMISSION_TYPE_DM')}}"
        ),
        SQLQueries.delete_table_sql.format(
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
            "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','PERMISSION_TYPE_DM')}}"
        )
    ],
    dag=dag
)
copy_app_fact = S3ToRedshiftOperator(
    task_id='copy_app_fact',
    s3_bucket="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'S3','TARGET_BUCKET')}}",
    aws_conn_id="aws_default",
    redshift_conn_id="postgres_default",
    schema="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
    s3_key="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_FACT_FT')}}",
    table="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_FACT_FT')}}",
    copy_options=copy_options,
    dag=dag
)
copy_app_category = S3ToRedshiftOperator(
    task_id='copy_app_category',
    s3_bucket="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'S3','TARGET_BUCKET')}}",
    aws_conn_id="aws_default",
    redshift_conn_id="postgres_default",
    schema="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
    s3_key="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DL_TABLES','APP_CATEGORY_TBL')}}",
    table="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_CATEGORY_DM')}}",
    copy_options=copy_options,
    dag=dag
)
copy_currency_type = S3ToRedshiftOperator(
    task_id='copy_currency_type',
    s3_bucket="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'S3','TARGET_BUCKET')}}",
    aws_conn_id="aws_default",
    redshift_conn_id="postgres_default",
    schema="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
    s3_key="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DL_TABLES','CURRENCY_TYPE_TBL')}}",
    table="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CURRENCY_TYPE_DM')}}",
    copy_options=copy_options,
    dag=dag
)
copy_developer = S3ToRedshiftOperator(
    task_id='copy_developer',
    s3_bucket="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'S3','TARGET_BUCKET')}}",
    aws_conn_id="aws_default",
    redshift_conn_id="postgres_default",
    schema="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
    s3_key="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DL_TABLES','DEVELOPER_TBL')}}",
    table="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','DEVELOPER_DM')}}",
    copy_options=copy_options,
    dag=dag
)
copy_content_rating = S3ToRedshiftOperator(
    task_id='copy_content_rating',
    s3_bucket="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'S3','TARGET_BUCKET')}}",
    aws_conn_id="aws_default",
    redshift_conn_id="postgres_default",
    schema="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
    s3_key="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DL_TABLES','CONTENT_RATING_TBL')}}",
    table="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CONTENT_RATING_DM')}}",
    copy_options=copy_options,
    dag=dag
)
copy_permission_type = S3ToRedshiftOperator(
    task_id='copy_permission_type',
    s3_bucket="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'S3','TARGET_BUCKET')}}",
    aws_conn_id="aws_default",
    redshift_conn_id="postgres_default",
    schema="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
    s3_key="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DL_TABLES','PERMISSION_TYPE_TBL')}}",
    table="{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','PERMISSION_TYPE_DM')}}",
    copy_options=copy_options,
    dag=dag
)
check_fact_app_cnt = SQLCheckOperator(
    task_id="check_fact_app_cnt",
    conn_id = "postgres_default",
    sql= SQLQueries.check_table_count_only.format(
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_FACT_FT')}}"
    ),
    dag=dag
)
check_app_category_qlt = SQLCheckOperator(
    task_id="check_app_category_qlt",
    conn_id = "postgres_default",
    sql= SQLQueries.check_table_count_duplicates.format(
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_CATEGORY_DM')}}",
        "Category_Desc",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','APP_CATEGORY_DM')}}",
        "Category_Desc"
    ),
    dag=dag
)
check_currency_type_qlt = SQLCheckOperator(
    task_id="check_currency_type_qlt",
    conn_id = "postgres_default",
    sql= SQLQueries.check_table_count_duplicates.format(
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CURRENCY_TYPE_DM')}}",
        "Currency_Type_Desc",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CURRENCY_TYPE_DM')}}",
        "Currency_Type_Desc"
    ),
    dag=dag
)
check_developer_qlt = SQLCheckOperator(
    task_id="check_developer_qlt",
    conn_id = "postgres_default",
    sql= SQLQueries.check_table_count_duplicates.format(
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','DEVELOPER_DM')}}",
        "Developer_Name",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','DEVELOPER_DM')}}",
        "Developer_Name"
    ),
    dag=dag
)
check_content_rating_qlt = SQLCheckOperator(
    task_id="check_content_rating_qlt",
    conn_id = "postgres_default",
    sql= SQLQueries.check_table_count_duplicates.format(
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CONTENT_RATING_DM')}}",
        "Cont_Rating_Desc",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','CONTENT_RATING_DM')}}",
        "Cont_Rating_Desc"
    ),
    dag=dag
)
check_permission_type_qlt = SQLCheckOperator(
    task_id="check_permission_type_qlt",
    conn_id = "postgres_default",
    sql= SQLQueries.check_table_count_duplicates.format(
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_SCHEMA','MODEL_SCH')}}",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','PERMISSION_TYPE_DM')}}",
        "Permission_Type_Desc",
        "{{getConfigValue(task_instance.xcom_pull(task_ids='config_retrieval', key='return_value'),'DWH_TABLES','PERMISSION_TYPE_DM')}}",
        "Permission_Type_Desc"
    ),
    dag=dag
)


create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

S3FirstLayer = EmrAddStepsOperator(
    task_id='first_layer_s3',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_FIRST_STEP,
    dag=dag
)

first_step_checker = EmrStepSensor(
    task_id='watch_first_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('first_layer_s3', key='return_value')[1] }}",
    aws_conn_id='aws_default',
    dag=dag
)

S3SecondLayer = EmrAddStepsOperator(
    task_id='second_layer_s3',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_SECOND_STEP,
    dag=dag
)

second_step_checker = EmrStepSensor(
    task_id='watch_second_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('second_layer_s3', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)
start_operator >> create_emr_cluster >> S3FirstLayer >> first_step_checker
first_step_checker >> S3SecondLayer >> second_step_checker >> terminate_emr_cluster
terminate_emr_cluster >> get_config >> create_schema
create_schema >> create_purge_app_fact >> copy_app_fact >> check_fact_app_cnt
create_schema >> create_purge_app_category >> copy_app_category >> check_app_category_qlt
create_schema >> create_purge_currency_type >> copy_currency_type >> check_currency_type_qlt
create_schema >> create_purge_developer >> copy_developer >> check_developer_qlt
create_schema >> create_purge_content_rating >> copy_content_rating >> check_content_rating_qlt
create_schema >> create_purge_permission_type >> copy_permission_type >> check_permission_type_qlt
check_fact_app_cnt >> end_operator
check_app_category_qlt >> end_operator
check_currency_type_qlt >> end_operator
check_developer_qlt >> end_operator
check_content_rating_qlt >> end_operator
check_permission_type_qlt >> end_operator
