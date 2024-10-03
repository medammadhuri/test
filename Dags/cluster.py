#Airflow End to End Automation

#Author : Rajeshwar

#Created on : 03-04-2024 


#Section-1: Importing Libraries


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
import time
import configparser
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.exceptions import AirflowFailException
import os
from sys import path
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from mail import send_email_on_failure, send_email_on_retry, send_email_on_success
import json
from airflow.models import Variable
import requests
import pytz
import pendulum 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import yaml
#from airflow.models import DAG


#SECTION 2 -- All define function calling the Task

# Set the Indian timezone
indian_timezone = pendulum.timezone('Asia/Kolkata')


#read the config file and getting the below details    
s3_client = boto3.client('s3')

# Config file path on S3
config_file_s3_path = 'orchstn_config_files/Airflow_config.ini'

S3_Bucket="analytics-olap-data-lake"

# getobject the config file from S3
response = s3_client.get_object(Bucket=S3_Bucket, Key=config_file_s3_path)

# Read the contents of the file
config_file_contents = response['Body'].read().decode('utf-8')

# Parse the config file
config = configparser.ConfigParser()
config.read_string(config_file_contents)


git_link=config.get('OLAP_pipeline', 'git_link')
git_branch=config.get('OLAP_pipeline', 'git_branch')
s3_path=config.get('OLAP_pipeline', 's3_path')
emr_path=config.get('OLAP_pipeline', 'emr_path')


#Start_time (printing time)
        
def start_time():
    #just print the time    
    start_time = datetime.now()
    print(start_time)
    print('ccccccc',git_link)
    print('checkkkkk',s3_path)
    
    



#end_time (printing time)   

def end_time():
    #just print the time 
    end_time = datetime.now()
    print(end_time)
    cluster_id = kwargs['ti'].xcom_pull(task_ids='Create_emr_cluster', key='cluster_id')
    print('clusterrrrrrr',cluster_id)
    
    
    
#Create EMR cluster:


def Create_emr_cluster(**kwargs):
    '''
    create the emr cluster based on below configuuration
    spark configuration and BootstrapActions details path can set the cluster itself
    And the cluster starting to bootstap action all are complete then only code deploy to that cluster
    so take call back the status of cluster once cluster is go to waiting stage then the function return cluster_id
    ''' 
    

    cluster_config={"Name": "olap_emr_spark","LogUri":"s3://analytics-olap-data-lake/logs/olap_emr_spark/", "ReleaseLabel": "emr-6.15.0","ServiceRole": "arn:aws:iam::590857988879:role/olap_emr_service_role","JobFlowRole": "olap_emr_ec2_role","Instances": {"Ec2SubnetIds": ["subnet-0cbac9d4cd934f175"],"InstanceGroups": [{"Name": "MasterInstanceGroup","InstanceRole": "MASTER","InstanceCount": 1,"InstanceType": "m6g.xlarge"},{"Name": "CoreInstanceGroup","InstanceRole": "CORE","InstanceCount": 3,"InstanceType": "m6g.xlarge"}],"Ec2KeyName": "platform_data_analytics","KeepJobFlowAliveWhenNoSteps": True,"TerminationProtected": False,"EmrManagedMasterSecurityGroup": "sg-0a697559492c15427","EmrManagedSlaveSecurityGroup": "sg-065dddeb7da510130","ServiceAccessSecurityGroup": "sg-0907224c8b6199294","AdditionalMasterSecurityGroups": ["sg-0d1cf6befcbe0a60b"],"AdditionalSlaveSecurityGroups": ["sg-0b5f6b6dc3fd300c7"]},"Applications": [{"Name": "JupyterEnterpriseGateway"},{"Name": "Spark"},{"Name": "Ganglia"}],"Configurations": [{"Properties": {"spark.sql.parquet.writeLegacyFormat": "true","spark.yarn.appMasterEnv.DATA_LAKE": "s3://analytics-olap-data-lake","spark.driver.memory": "4g","spark.executor.memory": "4g","spark.executor.cores": "4","spark.sql.autoBroadcastJoinThreshold": "10485760","spark.sql.shuffle.partitions": "50"},"Classification": "spark-defaults"},{"Classification": "spark","Properties": {"maximizeResourceAllocation": "true"}}],"BootstrapActions": [{"Name": "pre-requisites","ScriptBootstrapAction": {"Path": "s3://analytics-olap-data-lake/emr_config_files/emr_spark/spark_bootstrap_packages.sh","Args": []}}],"Tags": [{"Key": "Role","Value": "olap-processing-engine"},{"Key": "Owner","Value": "Platform"},{"Key": "Organization","Value": "Platform"},{"Key": "Business_Unit","Value": "Platform-Engineering"},{"Key": "Env","Value": "iqa"},{"Key": "Name","Value": "mu_spark_iqa_proc"},{"Key": "map-migrated","Value": "migXM7K8IMVNP"}],"VisibleToAllUsers": True}

    #cluster_id taken by jobflow
    response = client.run_job_flow(**cluster_config)
    cluster_id = str(response['JobFlowId'])


    ti = kwargs['ti']
    ti.xcom_push(key='cluster_id', value=cluster_id) 

    config.set('OLAP_pipeline',"cluster_id_dqm",str(cluster_id))

    # Write the modified config to a temporary file
    temp_config_file = StringIO()
    config.write(temp_config_file)
    
    # Upload the modified config file back to S3
    s3_client.put_object(Bucket=S3_Bucket, Key=config_file_s3_path, Body=temp_config_file.getvalue())   

    ti = kwargs['ti']
    ti.xcom_push(key='cluster_id', value=cluster_id) 
    
    while True:
        print("Checking cluster status...")
        response = client.describe_cluster(ClusterId=cluster_id)
        print("Current cluster status:", response['Cluster']['Status']['State'])
        if response['Cluster']['Status']['State'] == 'WAITING' or response['Cluster']['Status']['State'] == 'Waiting':
            print("Cluster is running!")
            break
        elif 'TERMINATED' in response['Cluster']['Status']['State'] or 'Terminate' in response['Cluster']['Status']['State']:
            print("create cluster is error")
            error_message = "create cluster is error"
            raise Exception(error_message)
            break
            
        time.sleep(10)
        
    return cluster_id

def git_clone_task(**kwargs):
    '''
    Function to install Git and clone the repository passs the cluster_id and emr commend run the install and clone
    then clone script in emr_cluster so copy to emr_cluster to s3
    '''

    #details get from config file   
    workspace=config.get('OLAP_pipeline', 'workspace')    
    region_name=config.get('OLAP_pipeline', 'region_name')
    ServiceRole1=config.get('OLAP_pipeline', 'ServiceRole')
    cluster_id=config.get('OLAP_pipeline', 'cluster_id_dqm')
    
    # Initialize the Boto3 EMR client using IAM role credentials
    emr_client = boto3.client('emr', region_name=region_name)

    # Define your EMR cluster ID
    cluster_id=config.get('OLAP_pipeline', 'cluster_id_dqm')

    if True:
        # Install Git on the EMR cluster
        install_git_command = f'sudo yum install -y git'
        step1=emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[{'Name': 'InstallGitStep', 'ActionOnFailure': 'CONTINUE', 'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['bash', '-c', install_git_command]}}])

        step_id = step1['StepIds'][0]
        
        if True:
            while True:
                time.sleep(5)
                response=emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
                step_status = response['Step']['Status']['State']
                if step_status =="COMPLETED":
                    print("statussssss",step_status)
                    break
                elif step_status =="FAILED":
                    error_message = "Git_clone is Failed."
                    raise Exception(error_message)
                    break
                print(step_status)


                
# Clone the repository on the EMR cluster

        git_clone_command = f'git clone -b {git_branch} {git_link} {emr_path}'
        step2=emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[{'Name': 'GitCloneStep', 'ActionOnFailure': 'CONTINUE', 'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['bash', '-c', git_clone_command]}}])
        step_id = step2['StepIds'][0]        
        
        if True:
            while True:
                time.sleep(5)
                response=emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
                step_status = response['Step']['Status']['State']
                if step_status =="COMPLETED":
                    print("statussssss",step_status)
                    break
                elif step_status =="FAILED":
                    error_message = "Git_clone is Failed."
                    raise Exception(error_message)
                    break
                print(step_status)

        # Execute a command on the EMR cluster
        s3_cp_command =f'aws s3 cp {emr_path} {s3_path} --recursive'
        step3=emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[{'Name': 'CopyToS3Step', 'ActionOnFailure': 'CONTINUE', 'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': ['bash', '-c', s3_cp_command]}}])
        step_id = step3['StepIds'][0]        
        
        if True:
            while True:
                time.sleep(5)
                response=emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
                step_status = response['Step']['Status']['State']
                if step_status =="COMPLETED":
                    print("statussssss",step_status)
                    break
                elif step_status =="FAILED":
                    error_message = "Git_clone is Failed."
                    raise Exception(error_message)
                    break
                print(step_status)




#Execute_jupyter_notebook all task execute note book on this define function

def execute_jupyter_notebook(path,**kwargs):
    '''
    Get the jupyter notebook path and cluster_id, then  workspace and details get from config file and execute the notebook
    Once notebook is start to finish monitor the stage and get failed or finish then only the loop is break.
    return the notebook stage
    '''

    #details get from config file   
    workspace=config.get('OLAP_pipeline', 'workspace')    
    aws_region=config.get('OLAP_pipeline', 'region_name')
    ServiceRole1=config.get('OLAP_pipeline', 'ServiceRole')
    cluster_id=config.get('OLAP_pipeline', 'cluster_id_dqm')


    region_name = aws_region
    workspace=config.get('OLAP_pipeline', 'workspace')

    # Initialize the Boto3 EMR client using IAM role credentials
    emr = boto3.client('emr', region_name=region_name)

    response = emr.start_notebook_execution(EditorId=workspace,RelativePath=path,NotebookExecutionName='spark',ExecutionEngine={'Id':cluster_id,'Type': 'EMR'},ServiceRole = ServiceRole1)

    # Get the execution ID
    execution_id = (response.get('NotebookExecutionId'))
    print(execution_id)
    
    # Poll the execution status until it's completed
    while True:
        execution_info = emr.describe_notebook_execution(NotebookExecutionId=execution_id)
        notebook_execution = execution_info['NotebookExecution']
        status = notebook_execution['Status']
        
        if status == 'FINISHED':
            print("Jupyter notebook execution completed successfully.")
            break
        elif status == 'FAILED':
            print("Jupyter notebook execution failed or was cancelled.")
            error_message = "Jupyter notebook execution failed."
            raise Exception(error_message)
            break
        else:
            print(f"Jupyter notebook execution is in progress (Status: {status}). Waiting...")
            time.sleep(5)  

    return status
    
     
#Terminate the EMR cluster:
 
def Terminate_cluster(**kwargs):
    '''
    All task is execute then terminate the cluster, 
    so take cluster_id and call the terminate job flows to pass the cluster_id
    Cluster will be terminate then task is completed 
    '''
    
    #details get from config file 
    cluster_id=config.get('OLAP_pipeline', 'cluster_id_dqm')
    region_name=config.get('OLAP_pipeline', 'region_name')
    
    
    #terminate the cluster
    emr_client = boto3.client('emr', region_name=region_name)
    response = emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    
    
def Terminate(cluster_id, region_name):
    '''
    All task is execute then terminate the cluster, 
    so take cluster_id and call the terminate job flows to pass the cluster_id
    Cluster will be terminate then task is completed 
    '''    
    #terminate the cluster
    emr_client = boto3.client('emr', region_name=region_name)
    response = emr_client.terminate_job_flows(JobFlowIds=[cluster_id])    
    
    
def refresh_failure(context, dag_name, task_name):
    '''
    Terminate after task get failed and send failure mail.
    '''
    dag_Name=dag_name
    task_Name=task_name
    send_email_on_failure(context,dag_Name,task_Name)
    cluster_id=config.get('OLAP_pipeline', 'cluster_id_dqm')
    print('clusterrrrrrrrrrrrrr',cluster_id)
    region_name = config.get('OLAP_pipeline', 'region_name')
    
    # Call terminate function
    Terminate(cluster_id, region_name)
   
   

#SECTION-3  DAG1-- schedule set the functionlity 


#Dag arugument

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 21, 9, 50, tzinfo=indian_timezone),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cluster',
    default_args=default_args,
    catchup=False,
    schedule_interval='30 14 * * 1-5'
) as cluster:


#Entier dag use python operator and call the perticular function and get response and send the mail
#Each task retry count on failure = 5; Retry_interval="5 min"
#Email will be triggered on Success,Failure and Retry	of each task


#Emr cluster creation
    Create_emr_cluster = PythonOperator(
        task_id='Create_emr_cluster',
        python_callable=Create_emr_cluster,
        on_failure_callback=lambda context: send_email_on_failure(context, 'cluster', 'Create_emr_cluster'),
        on_retry_callback=lambda context: send_email_on_retry(context, 'cluster', 'Create_emr_cluster'),
        on_success_callback=lambda context: send_email_on_success(context, 'cluster', 'Create_emr_cluster'),
        dag=cluster
    )

#git_clone_task
    git_clone_task = PythonOperator(
        task_id='git_clone_task',
        python_callable=git_clone_task,
        on_failure_callback=lambda context: send_email_on_failure(context, 'cluster', 'git_clone_task'),
        on_retry_callback=lambda context: send_email_on_retry(context, 'cluster', 'git_clone_task'),
        on_success_callback=lambda context: send_email_on_success(context, 'cluster', 'git_clone_task'),
        dag=cluster
    )


#start Time
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_time,
        dag=cluster
    )
    
#start Time
    end_task = PythonOperator(
        task_id='end_task',
        python_callable=start_time,
        dag=cluster
    )
		
   
# Trigger the 'DAG2_PS' DAG using triggerdagrunoperator for triggering purpose
 
    # Trigger the 'DAG3_PS' DAG after the 'DAG1_PS_demo' DAG runs successfully
    trigger_cluster = TriggerDagRunOperator(
        task_id='trigger_cluster1',
        trigger_dag_id='cluster1',
        on_success_callback=lambda context: send_email_on_success(context, 'cluster', "Transactional dashboard report is Refreshed"),
        on_failure_callback=lambda context: send_email_on_failure(context, 'cluster', "Transactional dashboard report is Refreshed"),
        on_retry_callback=lambda context: send_email_on_retry(context, 'cluster', "Transactional dashboard report is Refreshed"),
        dag=cluster,
        )

# Define the dependency of tasks in dag1
    	

    start_task >> Create_emr_cluster >> git_clone_task >> end_task>> trigger_cluster



# SECTION-4   Dag2--TASK EXECUTION AND SET the TASK depentencies 

#Entier dag use python operator and call the perticular function and get response and send the mail
#Each task retry count on failure = 5; Retry_interval="5 min"
#Email will be triggered on Success,Failure and Retry	of each task


# Define another DAG with the appropriate indentation
    with DAG(
        'cluster1',
        default_args=default_args,
        catchup=False,
        schedule_interval=None
    ) as cluster1:

# BR_Transaction_Detail

        BR_Transaction_Detail = PythonOperator(
            task_id='BR_Transaction_Detail',
            provide_context=True,
            python_callable=execute_jupyter_notebook,
            op_args=['/P2O/BR_Transaction_Detail_ps.ipynb'],
            on_failure_callback=lambda context: send_email_on_failure(context, 'cluster1', 'BR_Transaction_Detail'),
            on_retry_callback=lambda context: send_email_on_retry(context, 'cluster1', 'BR_Transaction_Detail'),
            on_success_callback=lambda context: send_email_on_success(context, 'cluster1', 'BR_Transaction_Detail'),
            execution_timeout=timedelta(seconds=7200),
            dag=cluster1,
        )
        
        
        
                
#terminate_cluster
        Terminate_cluster = PythonOperator(
            task_id='Terminate_cluster',
            provide_context=True,
            python_callable=Terminate_cluster,
            on_success_callback=lambda context: send_email_on_success(context, 'cluster1', 'Terminate_cluster'),
            on_failure_callback=lambda context: send_email_on_failure(context, 'cluster1', 'Terminate_cluster'),
            on_retry_callback=lambda context: send_email_on_retry(context, 'cluster1', 'Terminate_cluster'),
            dag=cluster1
        )	

        BR_Transaction_Detail>>Terminate_cluster
