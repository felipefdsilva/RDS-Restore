# Author: Felipe Ferreira da Silva
# Date: 09/08/2019
# Description: This function restores an RDS DB from an snapshot of a production database
# THIS FUNCTION WORKS WITH BOTO 1.9.20x 

import boto3
import json
import sys
from arnparse import arnparse

class RDSCopyException (Exception):
    pass

#Retrieves a client from the corresponding account
def get_client (service, account_number, region):

    ipsense_account_number = '025239092240'
    role_session_name = 'ipsense-rds-restore'
    role_name = 'multiview-ipsense'

    if (account_number == ipsense_account_number):
        return boto3.client(service, region_name=region)

    #Getting credentials to assume role
    account_credentials = boto3.client('sts').assume_role ( 
        RoleArn = 'arn:aws:iam::{accountNumber}:role/{role}'.
                    format(accountNumber=account_number, role=role_name),
        RoleSessionName = role_session_name
    )['Credentials']

    #Instantiating client
    external_client = boto3.client(service,
        aws_access_key_id = account_credentials['AccessKeyId'], 
        aws_secret_access_key = account_credentials['SecretAccessKey'], 
        aws_session_token = account_credentials['SessionToken'],
        region_name=region
    )
    return external_client

#Retrieves the rds snapshot list and return the latest one
def get_latest_snapshot (rds_arn):

    arn = arnparse(rds_arn)
    rds = get_client(arn.service, arn.account_id, arn.region)

    snapshot_list = rds.describe_db_snapshots(
        DBInstanceIdentifier=arn.resource
    )['DBSnapshots']

    if (len(snapshot_list) == 0):
        raise RDSCopyException ("No snapshot for this db instance")

    snapshot_list = sorted(snapshot_list, key=lambda snapshot: snapshot['InstanceCreateTime'])

    return snapshot_list[-1]

#Return the tags of a RDS instance
def get_tags (rds_arn):

    arn = arnparse(rds_arn)
    rds = get_client(arn.service, arn.account_id, arn.region)

    return rds.list_tags_for_resource (ResourceName=rds_arn)['TagList']

#Returns the description of a rds instance
def get_rds_description (rds_arn):

    arn = arnparse(rds_arn)
    rds = get_client(arn.service, arn.account_id, arn.region)
    
    rds_description = rds.describe_db_instances(DBInstanceIdentifier=arn.resource)['DBInstances'][0]
    rds_description['Tags'] = get_tags(rds_arn)

    return rds_description

#Deletes a RDS
def delete_rds (rds_arn):

    arn = arnparse(rds_arn)
    rds = get_client(arn.service, arn.account_id, arn.region)

    rds.delete_db_instance (
        DBInstanceIdentifier=arn.resource,
        SkipFinalSnapshot=True,
        DeleteAutomatedBackups=False
    )
    rds.get_waiter('db_instance_deleted').wait(DBInstanceIdentifier=arn.resource)

#Creates a RDS from the latest snapshot
def create_rds (snapshot_name, rds_description):

    arn = arnparse(rds_description['DBInstanceArn'])
    rds = get_client(arn.service, arn.account_id, arn.region)

    vpc_sec_groups_ids = []

    for sg in rds_description['VpcSecurityGroups']:
        vpc_sec_groups_ids.append(sg['VpcSecurityGroupId'])

    rds.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=rds_description['DBInstanceIdentifier'],
        DBSnapshotIdentifier=snapshot_name,
        DBInstanceClass=rds_description['DBInstanceClass'],
        #AvailabilityZone=rds_description['AvailabilityZone'], #se MultiAZ=True, gera erro
        DBSubnetGroupName=rds_description['DBSubnetGroup']['DBSubnetGroupName'],
        MultiAZ=rds_description['MultiAZ'],
        PubliclyAccessible=rds_description['PubliclyAccessible'],
        AutoMinorVersionUpgrade=rds_description['AutoMinorVersionUpgrade'],
        LicenseModel=rds_description['LicenseModel'],
        Engine=rds_description['Engine'],
        OptionGroupName=rds_description['OptionGroupMemberships'][0]['OptionGroupName'],
        StorageType=rds_description['StorageType'],
        VpcSecurityGroupIds=vpc_sec_groups_ids,
        CopyTagsToSnapshot=rds_description['CopyTagsToSnapshot'],
        EnableIAMDatabaseAuthentication=rds_description['IAMDatabaseAuthenticationEnabled'],
        DBParameterGroupName=rds_description['DBParameterGroups'][0]['DBParameterGroupName'],
        DeletionProtection=rds_description['DeletionProtection'],
        Tags=rds_description['Tags']
    )
    #waiting deletion finalization
    waiter = rds.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier=rds_description['DBInstanceIdentifier'])

    return get_rds_description (rds_description['DBInstanceArn'])

def main (event, context):
    sns_topic_arn = 'arn:aws:sns:us-east-1:025239092240:ipsense-rds-restore-service'
    parsed_sns_arn = arnparse(sns_topic_arn)
    
    #Getting parameters passed trough the event
    prod_rds_arn = event['ProductionRDS']
    dev_rds_arn = event['DevelopmentRDS']

    try:
        print ("Retrieving latest snapshot")
        snapshot = get_latest_snapshot(prod_rds_arn)
        print ("Selected Snapshot: {name}".format(name=snapshot['DBSnapshotIdentifier']))

    except RDSCopyException as e:
        print (e)
        exit(1)

    print ("Saving database description")
    rds_description = get_rds_description(dev_rds_arn)
    
    get_client(parsed_sns_arn.service, parsed_sns_arn.account_id, parsed_sns_arn.region).publish(
        TopicArn=sns_topic_arn,
        Message="The database below is being restored\n"\
            + json.dumps(rds_description, indent=4, sort_keys=True, default=str)
    )

    print ("Deleting database")
    delete_rds(dev_rds_arn)

    print ("Creating database")
    new_rds = create_rds(snapshot['DBSnapshotIdentifier'], rds_description)

    print ("New database description")
    print (json.dumps(new_rds, default=str, indent=4, sort_keys=True))

    get_client(parsed_sns_arn.service, parsed_sns_arn.account_id, parsed_sns_arn.region).publish(
        TopicArn=sns_topic_arn,
        Message="Lambda function ipsense-rds-restore has finished its job"
    )