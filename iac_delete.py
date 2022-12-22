import configparser
import boto3

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

def delete_iam():
    
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
    delete_cluster()
    delete_iam()
