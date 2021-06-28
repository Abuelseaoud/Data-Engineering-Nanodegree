import pandas as pd
import boto3
import json
import configparser
import io
import os
import time
#get AWS parameter
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
REGION                = config.get('AWS','REGION')

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER_CONFIG","DWH_CLUSTER_IDENTIFIER")
DWH_CLUSTER_TYPE       = config.get("CLUSTER_CONFIG","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("CLUSTER_CONFIG","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("CLUSTER_CONFIG","DWH_NODE_TYPE")


DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

#create clients
ec2 = boto3.resource('ec2',region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

iam = boto3.client('iam',region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

redshift = boto3.client('redshift',region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

#Create Iam ROLE &attach policy
try:
    dwhRole =iam.create_role(Description = "Allows Redshift clusters to call AWS services on your behalf.",
                             RoleName=DWH_IAM_ROLE_NAME,
                             AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
                            )
except Exception as e:
    print(e)
    
try:
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']  
except Exception as e:
    print(e)
    
try:
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
except Exception as e:
    print(e)
    
    
#Create Redshift cluster
try:
    response = redshift.create_cluster(
        # HW
        ClusterType=DWH_CLUSTER_TYPE ,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #identifiers & credentials
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER ,
        DBName=DWH_DB,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        #parameter
        IamRoles=[roleArn] 
    )
except Exception as e:
    print(e)
    

#wait till cluster is available
status=redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']

while status !="available" :
    time.sleep(10)
    status=redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']

#get endpoint,ARN
DWH_ENDPOINT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
DWH_ROLE_ARN = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['IamRoles'][0]['IamRoleArn']

#open an incoming TCP port to access the cluster ednpoint
try:
    vpc = ec2.Vpc(id=redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['VpcId'])
    defaultSg = list(vpc.security_groups.all())[1]
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name, 
        CidrIp='0.0.0.0/0', 
        IpProtocol='TCP',  
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
    
#write missing elements
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
config['CLUSTER']['HOST']=DWH_ENDPOINT 
config['IAM_ROLE']['ARN']=DWH_ROLE_ARN
config.write(open('dwh.cfg','w'))

    
    



    


