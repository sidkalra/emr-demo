import os
from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago

DAG_ID = "emr-task"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'sidharth.kalra@iovio.com',
    'email_on_failure': True,
    'email_on_retry': True
}

SPARK_STEPS = [
    {
        'Name': 'EMR Test',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                '--py-files',
                's3://sid-emr-spark-application/script/emrpkg-0.0.1-py3.9.egg',
                's3://sid-emr-spark-application/script/__main__.py'
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'demo-cluster-airflow',
    'ReleaseLabel': 'emr-5.33.0',
    'LogUri': 's3n://sid-emr-logs',
    'Applications': [
        {
            'Name': 'Spark'
        }
    ],
    'Instances': {
        'InstanceFleets': [
            {
                'Name': 'MASTER',
                'InstanceFleetType': 'MASTER',
                'TargetSpotCapacity': 1,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': 'm4.large'
                    }
                ]
            },
            {
                'Name': 'CORE',
                'InstanceFleetType': 'CORE',
                'TargetSpotCapacity': 1,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': 'm4.large'
                    }
                ]
            }

        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False
    },
    'BootstrapActions': [],
    'Configurations': [
        {
            'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
            }
        }
    ],
    'VisibleToAllUsers': True,
    'JobFlowRole': 'sid-emr-instance-profile',
    'ServiceRole': 'sid-emr-service-role',
    'EbsRootVolumeSize': 32,
    'StepConcurrencyLevel': 1
}

with DAG(
    dag_id=DAG_ID,
    description='Amazon EMR Test',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(0),
    schedule_interval='@once',
    tags=['emr']
) as dag:
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    cluster_creator >> step_adder >> step_checker >> cluster_remover

