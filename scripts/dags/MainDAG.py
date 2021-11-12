from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
#from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

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
          catchup=False
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

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='End_execution',  dag=dag)
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
first_step_checker >> S3SecondLayer >> second_step_checker >> terminate_emr_cluster >> end_operator

