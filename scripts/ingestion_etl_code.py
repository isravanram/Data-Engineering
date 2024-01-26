
##########################################################################################
# ETL Pipeline to bring data to Redshift DWH from Teradata Source through Teradata FastExport
##########################################################################################
import sys
import teradatasql
import logging
import boto3
import json
import time
import os
from datetime import datetime
from boto3.s3.transfer import TransferConfig


S3_MULTIPART_CONFIG = TransferConfig(
                        multipart_threshold=1024 * 25, 
                        max_concurrency=10,
                        multipart_chunksize=1024 * 25,
                        use_threads=True
                    )

JOB_ID = int(os.environ['JOB_ID'])
JOB_NAME = os.environ['JOB_NAME']
TERADATA_SECRET_NAME = os.environ['TERADATA_SECRET_NAME']
REDSHIFT_SECRET_NAME = os.environ['REDSHIFT_SECRET_NAME']
ENV = os.environ['PREFIX']
RUN_ID = os.environ['RUN_ID']
REGION = 'ENCODEX'    
PROCESS_DATE = datetime.utcnow().strftime('%Y-%m-%d')
CONFIG_TABLE = 'ASBC.S133'
PARAM_TABLE =   'XYSZ.E426'
GENERAL_TABLE =  'LMSN.R798'
STATUS_TABLE = 'JSK.E987'


def update_batch_job_status(batch_run_id,execution_status,error_msg='NA',query_executed='NA',records_processed=0,file_path='NA'):
    end_time = datetime.now()
    update_query = f'update {STATUS_TABLE} set end_time = \'{end_time}\',execution_status = \'{execution_status}\',error_msg = \'{error_msg}\',query_executed = \'{query_executed}\',records_processed={records_processed},file_path = \'{file_path}\' where job_run_id= \'{batch_run_id}\''
    if not REDSHIFT_OBJ.executeOnlyQuery(update_query):
        logger.error('Failed updating the batch job status table.')
        return
    logger.info('Successfully updated the batch job status table')


def terminate_job(job_phase,error_msg):
    try:
        logger.info('--- ERROR BLOCK ---')
        logger.info(f'Job phase : {job_phase}')
        error_msg = error_msg.replace('\'','`')
        logger.error(f'Error message: {error_msg}.')
        update_batch_job_status(
                batch_run_id=BATCH_RUN_ID,
                execution_status = 'FAILED',
                error_msg=error_msg
            )
        logger.info(f'Stopping the job.')
        boto3.client('batch').terminate_job(jobId=BATCH_RUN_ID,reason=error_msg)
        exit(1)

    except Exception as e:
        logger.error(f'Error occured while terminating the job : {e}')
        exit(1)

""" FUNCTION TO FETCH SECRET MANAGER CREDS """
def get_secret(secret_name, region_name):    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secrets = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secrets:
            secret = get_secrets['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secrets['SecretBinary'])
            return decoded_binary_secret
    except Exception as e:
        error_msg = f'Exception occured while reading the secret manager creds : {e}.'
        terminate_job(job_phase='Secret Manager creds retrieval',error_msg=error_msg)


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
            logger.error(f'Exception occured while fetching the Redshift response details. {e}')
            logger.error('Stopping the job')
            exit(1)
            
        if response_details['Status'] == 'FINISHED':
            return True
        else:
            logger.info(f"Redshift Query Error: {response_details['Error'] }.")
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
                logger.info(f"Empty Redshift result set for the query: {query}.")
        else:
            logger.info(f'Stopping the job')
            exit(1)
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

""" MAIN BLOCK """
if __name__ == '__main__':
        job_start_time = datetime.utcnow()
        """ Configuring the Loggers """
        logger = logging.getLogger()
        for handler in logger.handlers:
            logger.removeHandler(handler)
        logger.setLevel(logging.INFO)
        std_channel = logging.StreamHandler(sys.stdout)
        std_channel.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
        std_channel.setFormatter(formatter)
        logger.addHandler(std_channel)
        batchdate = datetime.now().strftime("%Y-%m-%d")
        redshift_secret=get_secret(secret_name = REDSHIFT_SECRET_NAME, region_name= REGION)
        logger.info('Retrieved Redshift Secret Manager creds.')
        redshift_secret =  json.loads(redshift_secret)
        REDSHIFT_OBJ= RedshiftManager(redshift_secret)
        batch_status_query = f"select job_run_id from {STATUS_TABLE} where job_name = \'{JOB_NAME}\' and execution_status = 'TRIGGERED' order by start_time desc limit 1"
        batch_status_details = REDSHIFT_OBJ.executeReadQuery(batch_status_query)
        if not batch_status_details:
            logger.error('Batch run id not found in the status table')
        BATCH_RUN_ID = batch_status_details[0].get('job_run_id')
        try:
            logger.info('""" Retrieving Metadata from Redshift Audit tables """')
            config_query = f'select * from {CONFIG_TABLE} where job_id = {JOB_ID}'
            config_details = REDSHIFT_OBJ.executeReadQuery(config_query)
            param_query = f'select * from {PARAM_TABLE} where job_id = {JOB_ID} and param_name in (\'S3_TABLE_SUFFIX\',\'SPROC\',\'INCREMENTAL_OPERATOR\',\'LOCAL_DIRECTORY\') order by param_name asc'
            param_details = REDSHIFT_OBJ.executeReadQuery(param_query)
            general_query = f'select * from {GENERAL_TABLE} where param_name = \'S3_BUCKET\' and env like \'%{ENV}%\' '
            general_details = REDSHIFT_OBJ.executeReadQuery(general_query)      
            if not (config_details and len(param_details) == 4 and general_details):
                error_msg = f'Missing metadata for job_id {JOB_ID}. Please check the config and param metadata entries.'
                terminate_job(job_phase='Metadata Retrieval',error_msg=error_msg)
            try:
                metadata = {
                    'src_schema' : config_details[0]['src_schema'],
                    'src_table' : config_details[0]['src_table'],
                    'sql_query'  : config_details[0]['sql_query'],
                    'load_type'  : config_details[0]['load_type'],
                    'incremental_key' : config_details[0]['incremental_key'],
                    'incremental_key_value' : config_details[0]['incremental_key_value'],
                    'incremental_operator' : param_details[0]['param_value'],
                    'local_directory' : param_details[1]['param_value'],
                    's3_table_suffix' : param_details[2]['param_value'],
                    'sproc' : param_details[3]['param_value'],
                    's3_bucket' : general_details[0]['param_value'],
                    'group_id' : config_details[0]['group_id'],
                    'batch_run_id' : BATCH_RUN_ID               }
                metadata_info = metadata.copy()
                del metadata_info['sql_query']
                logger.info(f'Metadata info: {metadata_info}')
            except Exception as e:
                error_msg = f'Error while parsing key values for the metadata entries. {e}'
                terminate_job(job_phase='Metadata retrieval',error_msg=error_msg)
            mandatory_fields=['src_schema','src_table','load_type','s3_table_suffix','sproc','s3_bucket','local_directory','batch_run_id']
            for key in mandatory_fields:
                if not metadata[key]:
                    error_msg = f'Empty value for the metadata key \'{key}\''
                    terminate_job(job_phase='Metadata retrieval',error_msg=error_msg)
        except Exception as e:
            error_msg = f'Error while reading the metadata from Redshift. {e}'
            terminate_job(job_phase='Metadata retrieval',error_msg=error_msg)
        logger.info(f'Successfully retrieved metadata from Redshift.')

        try:
            logger.info('""" Retrieving Teradata DB credentials & Data load info """')
            teradata_secret = json.loads(get_secret(TERADATA_SECRET_NAME, REGION))
            tdata_db_conn = {
                "host": teradata_secret['db_host'],
                "user": teradata_secret['db_username'],
                "password": teradata_secret['db_password']
            }
            if metadata.get('load_type').lower() == 'full_load':
                teradata_query = f"{metadata.get('sql_query')} {metadata.get('incremental_key_value')}"
            elif metadata.get('load_type').lower() == 'incremental':            
                if not(metadata.get('incremental_key') and metadata.get('incremental_key_value') and metadata.get('incremental_operator') and metadata.get('sql_query')):
                    error_msg = f'Insufficient Incremental load details in the metadata'
                    terminate_job(job_phase='Teradata creds & load details retrieval',error_msg=error_msg)
                else:
                    teradata_query = f"{metadata.get('sql_query')} {metadata.get('incremental_key')} {metadata.get('incremental_operator')} {metadata.get('incremental_key_value')}"
            else:
                error_msg = f'Invalid Load type in the metadata.'
                terminate_job(job_phase='Teradata creds & load details retrieval',error_msg=error_msg)      
            logger.info(f'Successfully retrieved the DB creds and load type requirements.')

        except Exception as e:
            error_msg = f'Exception occured while fetching the metdata and connection details : {e}.'
            terminate_job(job_phase='Teradata creds & load details retrieval',error_msg=error_msg)
       
        try:
            logger.info(f' """ Executing FastExport query """ ')
            LOCAL_FILE = f"{metadata.get('local_directory')}/{JOB_NAME}.csv"
            logger.info(f' Teradata query : \n {teradata_query} \n')
            logger.info(f'Local file path to store the data : {LOCAL_FILE}')
            with teradatasql.connect(json.dumps(tdata_db_conn)) as con:
                    with con.cursor() as cur:
                        sSelect = "{fn teradata_try_fastexport}{fn teradata_write_csv(" + LOCAL_FILE + ")}" + teradata_query
                        cur.execute(sSelect)
                        records_count = cur.rowcount
            logger.info(f'Record count : {records_count}')
            if not records_count:
                error_msg =f'No new records found from the Teradata source for incremental load hence skipping S3 write.'
                terminate_job(job_phase='FastExport query execution',error_msg=error_msg)      
            logger.info(f'Successfully executed the Teradata query using Teradata FastExport and stored the data to local directory \'{LOCAL_FILE}\'.')
        except Exception as e:
            error_msg = f'Error occured while running the fast export query : {e}.'
            terminate_job(job_phase='FastExport query execution',error_msg=error_msg)

        try:
            logger.info('""" Copying data from Local file to S3 """')
            s3_url = f"{metadata.get('s3_table_suffix')}/{JOB_NAME}.csv"
            s3_resource = boto3.resource('s3')
            s3_bucket = f"{ENV}-{metadata.get('s3_bucket')}"
            logger.info(f'S3 bucket : {s3_bucket}, S3 url : {s3_url} ')
            s3_file_path = f'{s3_bucket}/{s3_url}'
            s3_resource.Object(s3_bucket, s3_url).upload_file(LOCAL_FILE,
                                ExtraArgs={'ContentType': 'text/csv'},
                                Config=S3_MULTIPART_CONFIG
                                )
            logger.info(f'Successfully written the data to S3 bucket {s3_bucket}')
        except Exception as e:
            error_msg = f'Error occured while writing the data to S3 bucket. {e}'
            terminate_job(job_phase='S3 data-write',error_msg=error_msg)
        
        if metadata.get('sproc') != 'NA':
            try:
                logger.info('""" Triggering the Stored Procedure """')
                sproc_name = metadata.get('sproc')
                sproc_query = f'call {sproc_name}'.format(run_id = JOB_NAME,s3_file_path = s3_file_path,load_type = metadata.get('load_type'),job_id=JOB_ID)
                logger.info(f'Sproc query : {sproc_query}')
                if not REDSHIFT_OBJ.executeOnlyQuery(sproc_query):
                    raise Exception
                logger.info('Successfully populated target table through stored procedure.')

            except Exception as e:
                error_msg = f'Error occured while calling the sproc.{e}'
                terminate_job(job_phase='Stored Procedure call',error_msg=error_msg)

        teradata_query = teradata_query.replace('\'','`')
        update_batch_job_status(
            batch_run_id=metadata.get('batch_run_id'),
            execution_status='SUCCESS',
            error_msg='NA',
            query_executed=teradata_query,
            records_processed=records_count,
            file_path=s3_file_path)     
    
        logger.info('""" ETL Pipeline executed successfully """')
     
      