# Lambda to trigger AWS ETL Batch job
	  
import base64
import json
import logging
import sys
import datetime
import boto3
import os
import time


""" CLASS FOR CONFIGURING REDSHIFT CREDS """
class RedshiftManager:
    def __init__(self,redshift_secret):
        self.redshift_cluster = redshift_secret['cluster_name']
        self.redshift_db = redshift_secret['database']
        self.redshift_username = redshift_secret['username']
        self.redshift_client = boto3.client('redshift-data')

    def getResponseDetails(self,response):
        try:
            time.sleep(5)
            response_details= self.redshift_client.describe_statement(Id = response)
            while (response_details['Status'] == 'STARTED' or response_details['Status'] == 'PICKED' or response_details['Status'] == 'SUBMITTED'):
                time.sleep(5)
                response_details= self.redshift_client.describe_statement(Id = response)

        except Exception as e:
            print('Exception occured while fetching the Redshift response details.')
            print(e)
            
        if response_details['Status'] == 'FINISHED':
            return True
        else:
            print(f"Error occured in redshift connection : {response_details['Error'] }.")
        return False

    def executeReadQuery(self,query):
        response = self.redshift_client.execute_statement(
            ClusterIdentifier = self.redshift_cluster,
            Database = self.redshift_db,
            DbUser = self.redshift_username,
            Sql = query
        )
        redshift_result_list = []
        response_id = response['Id']
        if self.getResponseDetails(response_id):
            response_get = self.redshift_client.get_statement_result(Id = response_id)
            if len(response_get['Records']) >= 1:
                columns = [c['label'] for c in response_get['ColumnMetadata']]
                result_set = response_get['Records']
                for record in result_set:
                    param_dict= {}
                    for i in range(len(columns)):
                        param_dict[columns[i]] = record[i].get('longValue') or record[i].get('stringValue')
                    redshift_result_list.append(param_dict)
            else:
                print(f"Empty Redshift result set for the query: {query}.")
        return redshift_result_list

    def executeOnlyQuery(self,query):
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.redshift_cluster,
            Database = self.redshift_db,
            DbUser = self.redshift_username,
            Sql = query
        )
        status = self.getResponseDetails(response['Id'])
        return status

def get_scrt(scrt_name, region_name):
    """ Function to retrieve secrets from secret manager"""
    # Create a Secret Manager Client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_scrts = client.get_secret_value(SecretId=scrt_name)
        if 'SJKX' in get_scrts:
            scrt = get_scrts['ABCX']
            return scrt
        else:
            decoded_binary_scrt = base64.b64decode(get_scrts['123X'])
            return decoded_binary_scrt
    except Exception as e:
        raise e

def lambda_handler(event, context):
    client = boto3.client('batch')
    GROUP_ID = event['group_id']
    REDSHIFT_SECRET_NAME = os.environ['REDSHIFT_SECRET_NAME']
    REGION_NAME = 'ENCODEX'
    secret_response=get_scrt(REDSHIFT_SECRET_NAME, REGION_NAME)
    redshift_secret = json.loads(secret_response)
    redshift_client = RedshiftManager(redshift_secret)
    collection_config_query = "select job_id from pwp_audit_control.teradata_job_config where group_id = '" + GROUP_ID + "' and UPPER(active_flag)='Y'"
    jobs = redshift_client.executeReadQuery(collection_config_query)
    JOB_STATUS_TABLE = 'S324.X345'
    for job in jobs:
        JOB_ID = job['job_id']
        JOB_TIME = datetime.datetime.utcnow()
        JOB_NAME = f"{JOB_ID}-{JOB_TIME.strftime('%Y-%m-%d_%H-%M-%S')}"
        print(f'\n """" Submitting job for {JOB_ID} """" ')
        if redshift_client.executeReadQuery(f'select * from {JOB_STATUS_TABLE} where job_id = {JOB_ID} and execution_status = \'TRIGGERED\''):
            print(f'The job having ID {JOB_ID} is already running hence skipping the job')
            continue
        try:
            response = client.submit_job(
                        jobDefinition=os.environ.get('JOB_DEFINITION'),
                        jobName=JOB_NAME,
                        jobQueue=os.environ.get('JOB_Q'),
                        containerOverrides={
                            'environment': [
                                {
                                    'name': 'REDSHIFT_SECRET_NAME',
                                    'value': os.environ.get('REDSHIFT_SECRET_NAME')
                                },
                                {
                                    'name': 'TERADATA_SECRET_NAME',
                                    'value': os.environ.get('TERADATA_SECRET_NAME')
                                },
                                {
                                    'name': 'JOB_ID',
                                    'value': f"{JOB_ID}"
                                },
                                {
                                    'name': 'ENVIRONMENT',
                                    'value': os.environ.get('ENVIRONMENT')
                                },
                                {
                                    'name': 'PREFIX',
                                    'value': os.environ.get('PREFIX')
                                },
                                {
                                    'name': 'JOB_NAME',
                                    'value': JOB_NAME
                                }
                            ]
                        }
                    )
            
            JOB_RUN_ID = response.get('jobId')
            status_update_query = f'insert into {JOB_STATUS_TABLE} (job_id,job_name,group_id,job_run_id,start_time,execution_status) values ({JOB_ID},\'{JOB_NAME}\',\'{GROUP_ID}\',\'{JOB_RUN_ID}\',\'{JOB_TIME}\',\'TRIGGERED\')'
            if not redshift_client.executeOnlyQuery(status_update_query):
                client.terminate_job(jobId=JOB_RUN_ID,reason='Error occured while updating the job status table from Lambda')
                raise Exception
            print(f'Batch job started for job_id \'{JOB_ID}\'. Job name : {JOB_NAME} ')
        except Exception as e:
            print(f'Error while submitting the job having ID {JOB_ID}. {e}')
            client.terminate_job(jobId=JOB_RUN_ID,reason='Error occured while updating the job status table from Lambda')
            return 'Error occured while submitting tbatch job'
    return "Successful"
	
	





