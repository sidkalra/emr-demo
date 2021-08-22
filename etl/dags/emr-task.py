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

SPARK_STEPS = [{
    'Name': 'EMR Test',
    'ActionOnFailure': 'CONTINUE',
}]
