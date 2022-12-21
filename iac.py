import configparser
import boto3
import pandas as pd
import json
import time

def create_iam():

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    DWH_IAM_ROLE_NAME = config.get('CLUSTER', 'DWH_IAM_ROLE_NAME')

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    try:
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'}
            )
        )    
        print('Role Created!')
    except Exception as e:
        print(e)
        
    try:
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        config.set('IAM_ROLE', 'ARN', roleArn)

        print('Role Policy Attached!')

    except Exception as e:
        print(e)

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

def create_redshift_cluster():

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get('CLUSTER', 'DWH_CLUSTER_TYPE')
    DWH_NODE_TYPE = config.get('CLUSTER', 'DWH_NODE_TYPE')
    DWH_NUM_NODES = config.get('CLUSTER', 'DWH_NUM_NODES')
    DWH_DB = config.get('CLUSTER', 'DWH_DB')
    DWH_DB_USER = config.get('CLUSTER', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('CLUSTER', 'DWH_DB_PASSWORD')
    DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
    DWH_PORT = config.get('CLUSTER', 'DWH_PORT')
    roleArn = config.get('IAM_ROLE', 'ARN')

    redshift = boto3.client(
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    try:
        redshift.create_cluster(        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

    fl_created = False
    seconds_to_stop = 500

    while fl_created == False and seconds_to_stop > 0:
        time.sleep(15)
        redshift_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        redshift_status = redshift_props['ClusterStatus']
        if redshift_status != 'available':
            print('Creating redshift')
            seconds_to_stop = seconds_to_stop - 15
        else:
            fl_created = True
            print('Redshift created!')
            redshift_endpoint = redshift_props['Endpoint']['Address']
            redshift_vpcid = redshift_props['VpcId']
    
    config.set('CLUSTER', 'HOST', redshift_endpoint)

    ec2 = boto3.resource(
        'ec2',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    try:
        vpc = ec2.Vpc(id=redshift_vpcid)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
    
    return redshift
    
def delete_cluster():

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    redshift = boto3.client(
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


def delete_iam(iam):
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    DWH_IAM_ROLE_NAME = config.get('CLUSTER', 'DWH_IAM_ROLE_NAME')

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    return None

if __name__ == '__main__':
    create_iam()
    create_redshift_cluster()