"""
This program copy s3 file to ftp
"""
import sys
import logging
import datetime
import io
import json
import os
from ftplib import FTP_TLS
from io import BufferedReader
import boto3

FTP_HOST_NAME = os.environ['FTP_HOST']
FTP_PORT_NUMBER = int(os.environ['FTP_PORT_NUMBER'])
FTP_FOLDER_NAME = os.environ['FTP_FOLDER']
FTP_USER_NAME = 'BANBTA\\' + os.environ['FTP_USER']
FTP_ACCESS_KEY = os.environ['FTP_ACCESS']
LOAD_FILENAME = os.environ['FILENAME']
BUCKET_NAME = os.environ['BUCKETNAME']
NOW = datetime.datetime.now()
FILE_NAME = LOAD_FILENAME+NOW.strftime("%Y%m%d")
FILE_NAME = FILE_NAME+".csv"

"""
Return the logger for display information messages through the loggin library
"""
def get_logger():
    logger = logging.getLogger('Info')
    logger.setLevel(logging.INFO)
    logging.info('task')
    return logger

"""
This function is used to connect to the ftp server
"""
def ftp_connection():
    ftp_conn = None
    try:
        logger = get_logger()
        logger.info("Creating object FTP_TLS()")
        ftp_conn = FTP_TLS()

        logger.info("Trying to connect to FTP(%s, %d)", FTP_HOST_NAME, FTP_PORT_NUMBER)
        ftp_conn.connect(FTP_HOST_NAME, FTP_PORT_NUMBER)

        ftp_conn.auth()
        logger.info("auth() 0k")
        ftp_conn.prot_p()
        logger.info("prot_p() 0k")
        logger.info("Trying with FTP_USER_NAME = %s", FTP_USER_NAME)

        ftp_conn.login(user=FTP_USER_NAME, passwd=FTP_ACCESS_KEY)
        ftp_conn.set_debuglevel(2)

        logger.info('Connected')
    except Exception as e:
        logger.error('Not connected %s', e)

    return ftp_conn
"""
This function is used to check if the file exist in FTP
"""
def file_exist_in_ftp(ftp_conn):
    logger = get_logger()
    ftp_conn.cwd(FTP_FOLDER_NAME)
    logger.info('Success change directory')
    result = False
    try:
        result = ftp_conn.size(FILE_NAME) != 0
        if result:
            logger.info('File already exist')

    except Exception as e:
        logger.info('File not exists %s', e)
    
    return result

"""
This function find the file in the s3 and download it
"""
def find_file_in_s3():
    try:
        logger = get_logger()
        bucket_name = BUCKET_NAME
        s3_path = "/" + FTP_FOLDER_NAME + "/" + FILE_NAME
        logger.info("Searching file in: %s", s3_path)
        s3 = boto3.resource('s3')
        logger.info("FILENAME: %s", FILE_NAME)
        #s3_response_object = s3.get_object(Bucket=bucket_name, Key=s3_path)
        #object_content = s3_response_object['Body'].read()
        s3_get_file = s3.Bucket(bucket_name).download_file(s3_path, '/tmp/'+FILE_NAME)
        file = open('/tmp/'+FILE_NAME, 'rb')
        logger.info("File is ready %s", type(file))
        return file
    except Exception as error:
        logger.error('You don\'t have connection with s3 %s', error)
        return None
"""
This function is used to up the file downloaded from s3 to ftp server
"""
def up_file_to_ftp(ftp_conn, data_csv):
    logger = get_logger()
    logger.info('Trying to upload the file in FTP')
    if ftp_conn:
        file_name = FILE_NAME
        #bio = io.BytesIO(data_csv)
        logger.info('Writing file')
        ftp_conn.storlines('STOR ' + file_name, data_csv)
        logger.info('File published in FTP')

"""
This is the main function
"""
def lambda_handler(event, context):
    data_csv = find_file_in_s3()
    find_file_in_s3()
    if data_csv == None:
        logger = get_logger()
        logger.error("File not found in S3")
        sys.exit(1)

    ftp_server = ftp_connection()
    if ftp_server != None:
        if file_exist_in_ftp(ftp_server) is False:
            find_file_in_s3()
            up_file_to_ftp(ftp_server, data_csv)
            
        ftp_server.quit()
        ftp_server.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
