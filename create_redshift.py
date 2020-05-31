import os
import boto3

#load configuration file
from configparser import ConfigParser
config = ConfigParser()
config.read_file(open('dwh.cfg'))

#key and secret used for connecting to AWS
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')


#create client to connect to aws
from boto3 import client
iam = client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

# Create IAM Role for Redshift to have ReadOnly access to S3
from json import dumps
from botocore.exceptions import ClientError

DB_ROLE_NAME = config.get("CLUSTER", "DB_ROLE_NAME")

try:
    print("Creating new IAM Role")
    dwhRole = iam.create_role(
        Path='/',
        RoleName = DB_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=dumps(
            {'Statement':[{'Action': 'sts:AssumeRole',
                          'Effect':'Allow',
                          'Principal': {'Service': 'redshift.amazonaws.com'}}],
                         'Version':'2012-10-17'})
    )
except Exception as e:
    print(e)
    
# Attach policies to role
print('Attaching Policy')
iam.attach_role_policy(RoleName=DB_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

# Get Amazon resource name
print('Getting Amazon resource name')
roleArn = iam.get_role(RoleName=DB_ROLE_NAME)['Role']['Arn']

# Create Redshift client
from boto3 import client
print('Connecting to redshift client')
redshift = client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

# Load data variables to create database from configuration file
print("Loading variables to create database")
DB_CLUSTER_TYPE       = config.get("CLUSTER","DB_CLUSTER_TYPE")
DB_NUM_NODES          = config.get("CLUSTER","DB_NUM_NODES")
DB_NODE_TYPE          = config.get("CLUSTER","DB_NODE_TYPE")

DB_HOST               = config.get("CLUSTER","HOST")
DB_NAME               = config.get("CLUSTER","DB_NAME")
DB_USER               = config.get("CLUSTER","DB_USER")
DB_PASSWORD           = config.get("CLUSTER","DB_PASSWORD")
DB_PORT               = config.get("CLUSTER","DB_PORT")

# Create Redshift Database
print('Creating Redshift database')
try:
    response = redshift.create_cluster(        
        # Parameters for hardware
        ClusterType=DB_CLUSTER_TYPE,
        NodeType=DB_NODE_TYPE,
        NumberOfNodes=int(DB_NUM_NODES),
        
        # Parameters for identifiers & credentials
        DBName=DB_NAME,
        ClusterIdentifier=DB_HOST,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,
        
        # Parameter for role s3 access
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)
    
#Loop checks every minute to see if database is available


import time
starttime=time.time()
while (redshift.describe_clusters(ClusterIdentifier=DB_HOST)['Clusters'][0] == 'creating') == True:
    print("Checking every minute for redshift creation...")
    time.sleep(60.0 - ((time.time() - starttime) % 60.0))
    
# Save DB Endpoint and DB ROLE ARN 
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_HOST)['Clusters'][0]
DB_ENDPOINT = myClusterProps['Endpoint']['Address']
DB_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

print("Copy and save DWH_ENDPOINT and  DWH_ROLE_ARN for later")
print("DWH_ENDPOINT :: ", DB_ENDPOINT)
print("DWH_ROLE_ARN :: ", DB_ROLE_ARN)