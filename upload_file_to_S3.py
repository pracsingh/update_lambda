from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
import boto3
import psycopg2
from datetime import timedelta, datetime
s3 = boto3.resource('s3')
import psycopg2
import csv

csvFile =  open('/home/ec2-user/people.csv', 'r')
reader = csv.reader(csvFile)


#from airflow.operators import S3ToRedshiftOperator

def cloudformation():

    client = boto3.client('cloudformation',region_name='us-east-1')

    response = client.create_stack(
        StackName='airflowcloudformation',
        TemplateURL='https://s3-external-1.amazonaws.com/cloudformation-templates-us-east-1/WordPress_Single_Instance.template',
        Parameters=[
            {
                'ParameterKey': 'KeyName',
                'ParameterValue': 'airflow',
                'UsePreviousValue': False
                

            },
            {
                'ParameterKey': 'DBName',
                'ParameterValue': 'wordpressdb',
                'UsePreviousValue': False
                
            },
            {
                'ParameterKey': 'DBPassword',
                'ParameterValue': 'vsengar123',
                'UsePreviousValue': False
                
            },
            {
                'ParameterKey': 'DBRootPassword',
                'ParameterValue': 'vsengar123',
                'UsePreviousValue': False
                
            },
            {
                'ParameterKey': 'DBUser',
                'ParameterValue': 'vsengar',
                'UsePreviousValue': False
                
            },
            {
                'ParameterKey': 'InstanceType',
                'ParameterValue': 't1.micro',
                'UsePreviousValue': False
                
            },
            {
                'ParameterKey': 'SSHLocation',
                'ParameterValue': '0.0.0.0/0',
                'UsePreviousValue': False
                
            },

        
        ],
        TimeoutInMinutes=123,
        ResourceTypes=[
            'AWS::EC2::Instance',
        ],
        OnFailure='ROLLBACK',
        Tags=[
            {
                'Key': 'Name',
                'Value': 'airflowclod'
            },
            {
                'Key': 'Client',
                'Value': 'will update soon'
            },
            {
                'Key': 'CreatedBy',
                'Value': 'vsengar@teksystems.com'
            },
            {
                'Key': 'Initiative',
                'Value': 'Airflow task scheduler'
            },
            {
                'Key': 'Purpose',
                'Value': 'need to update'
            },
        ]
    )


def upload_file_to_S3(filename, key, bucket_name):
    s3.Bucket(bucket_name).upload_file(filename, key)

def redshift():

    conn = psycopg2.connect(dbname='dev', host='coe-etl-cluster.cdb0frmfrvwa.us-east-1.redshift.amazonaws.com', port='5439', user='awsuser', password='^8Y4<wmSb<CaA#J3')
    cur = conn.cursor();
    count = 0
    for row in reader:
        
        if count == 0:
            print("inside creating table")
            name = row[0]
            emp_id = row[1]
            print(row)
            try:
                cur.execute('CREATE TABLE IF NOT EXISTS raw_data.emp_detailss ({} VARCHAR,{} VARCHAR)'.format(name,emp_id))
                cur.execute('commit')
            except Exception as e:
                print(e)
            #cur.execute('INSERT INTO raw_data.user_detailss(name, id) VALUES (%s, %s)',("vipendra1", 24))
            #cur.execute('commit')
            count = count + 1
        
        else:
            name_value = str(row[0])
            emp_id_value = str(row[1])
            print("inside inserting data")
         
            cur.execute('INSERT INTO raw_data.emp_detailss(name,id) VALUES (%s, %s)',(name_value,emp_id_value))
            cur.execute('commit')
            cur.execute('SELECT * FROM raw_data.emp_detailss')
            for table in cur.fetchall():
                print(table)
            
    
    print('success ')


default_args = {
    'owner': 'vsengar',
    'start_date': datetime(2019, 4, 23),
    'retry_delay': timedelta(minutes=5)
}
# Using the context manager alllows you not to duplicate the dag parameter in each operator
with DAG('S3_dag_test_new_one', default_args=default_args, schedule_interval='@once') as dag:

    start_task = DummyOperator(
           task_id='dummy_start'
   )

    
    upload_to_S3_task = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_file_to_S3,
        op_kwargs={
            'filename': '/home/ec2-user/airflow/dags/new.csv',
            'key': 'new.csv',
            'bucket_name': 'aws-airflow',
        },
        dag=dag)
    
    
    local_to_redshift = PythonOperator(
        task_id='upload_to_redshift',
        python_callable=redshift,
        op_kwargs={},
        dag=dag
        )
        
    cloudformation = PythonOperator(
        task_id='airflow_cloudformation',
        python_callable=cloudformation,
        op_kwargs={},
        dag=dag
        )
       
    # Use arrows to set dependencies between tasks
#start_task >> upload_to_S3_task
start_task >> upload_to_S3_task >> local_to_redshift >> cloudformation