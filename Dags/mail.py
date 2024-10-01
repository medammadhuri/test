from datetime import datetime, timedelta
from airflow import DAG
import configparser
import os
import boto3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib




#EC2_Instance_IP

def  get_ec2_instance_ip():

    public_ip="10.172.88.148"

    return public_ip
    
port="31151"


#Failure_mail:

def send_email_on_failure(context, dag_name, task_name):
    '''
    If task is Failure means, we send the mail getting from dag context details and the mail include the retry count 
    the mail include the basic details of task and web UI link
    And we get Instance_ip from get_ec2_instance_ip function
    Email body set basic details.
    pass all detail to send email function, send to mail
    '''


    host_name=get_ec2_instance_ip()    

    email_subject = f"DAG Failure: {dag_name}, Task Failure: {task_name}"
    retry_count = context['task_instance'].try_number- 1
    
    # Airflow UI link
    ui_link = f"http://{host_name}:{port}/dags/{dag_name}/grid"
    email_body = (
        f"DAG Failed: {dag_name}, Task Failed: {task_name}\n"
        f"DAG execution failed for {context['execution_date']}\n"
        f"Retry count: {retry_count}\n"
        f"{ui_link}\n"
        f"Please check the Airflow UI for details\n"
        f"DAG Failed_details: {context['exception']}"
    )

    send_email(email_subject, email_body)


# Retry mail:

def send_email_on_retry(context, dag_name, task_name):
    '''
    If task is Retry means, we send the mail getting from dag context details and the mail include the retry count 
    the mail include the basic details of task and web UI link
    And we get Instance_ip from get_ec2_instance_ip function
    Email body set basic details.
    pass all detail to send email function, send to mail
    '''

     
    host_name=get_ec2_instance_ip()    
    email_subject = f"DAG Retry: {dag_name}, Task Retry: {task_name}"
    retry_count = context['task_instance'].try_number- 1
    # Airflow UI link
    ui_link = f"http://{host_name}:{port}/dags/{dag_name}/grid"
    email_body = (
        f"DAG Retry: {dag_name}, Task Retry: {task_name}\n"
        f"DAG execution Retry for {context['execution_date']}\n"
        f"Retry count: {retry_count}\n"
        f"{ui_link}\n"
        f"Please check the Airflow UI for details\n\n"
    )

    send_email(email_subject, email_body)


# Success mail:

def send_email_on_success(context, dag_name, task_name):     
    '''
    If task is Success means, we send the mail getting from dag context details and the mail include the retry count 
    the mail include the basic details of task and web UI link
    And we get Instance_ip from get_ec2_instance_ip function
    Email body set basic details.
    pass all detail to send email function, send to mail
    '''

     
    host_name=get_ec2_instance_ip()    
    email_subject = f"DAG Success: {dag_name}, Task Success: {task_name}"
    retry_count = context['task_instance'].try_number- 1
    
    # Airflow UI link
    ui_link = f"http://{host_name}:{port}/dags/{dag_name}/grid"
    email_body = (
        f"DAG Success: {dag_name}, Task Success: {task_name}\n"
        f"DAG execution succeeded for {context['execution_date']}\n"
        f"{ui_link}\n"
        f"Retry count: {retry_count}\n"
    )

    send_email(email_subject, email_body)



#Mailling part:

def send_email(subject, body):
    '''
    If all success or failure or retry all function calling the send mail function are passing the data
    and functionality then mail will be trigger here
    '''

    # Email configuration
    smtp_server = 'smtp.office365.com'
    smtp_port = 587
   
    sender_email="fss_analytics@fss.co.in"    
    receiver_email="fss_analytics@fss.co.in"    
    password="yrmytvzyvvsjlhgq"


    # Create a multipart message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject

    # Add the message body
    message.attach(MIMEText(body, 'plain'))

    try:
        # Create a secure SSL/TLS connection with the SMTP server
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()

            # Login to the SMTP server
            server.login(sender_email, password)

            # Send the email
            server.sendmail(sender_email, receiver_email, message.as_string())

        print('Email sent successfully!')
        
    except smtplib.SMTPException as e:
        print(f'Error sending email: {str(e)}')
