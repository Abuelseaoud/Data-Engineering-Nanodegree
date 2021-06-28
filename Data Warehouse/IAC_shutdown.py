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

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

#create clients

iam = boto3.client('iam',region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

redshift = boto3.client('redshift',region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
   
#delete Redshift cluster
try:
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
except Exception as e:
    print(e)
    



#open an incoming TCP port to access the cluster ednpoint
try:
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
except Exception as e:
    print(e)
    
#delete elements
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
config['CLUSTER']['HOST']= ''
config['IAM_ROLE']['ARN']=''
config.write(open('dwh.cfg','w'))

    
    



    


