import boto3
import json
#import re
import sys

def get_latest_snapshot (rds_name):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    snapshot_list = rds.describe_db_snapshots(
        DBInstanceIdentifier=rds_name
    )['DBSnapshots']

    snapshot_list = sorted(snapshot_list, key=lambda snapshot: snapshot['InstanceCreateTime'])

    return snapshot_list[-1]

def delete_rds (rds_name):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    deleted_rds = rds.delete_db_instance (
        DBInstanceIdentifier=rds_name,
        SkipFinalSnapshot=True,
        DeleteAutomatedBackups=False
    )
    return deleted_rds

def create_rds (snapshot_name):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    created_rds = rds.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=original_rds['DBInstanceIdentifier'],
        DBSnapshotIdentifier=snapshot_name,
        DBInstanceClass=original_rds['DBInstanceClass'],
        Port=original_rds['Port'],
        AvailabilityZone=original_rds['AvailabilityZone'],
        DBSubnetGroupName=original_rds['DBSubnetGroupName'],
        MultiAZ=original_rds['MultiAZ'],
        PubliclyAccessible=original_rds['PubliclyAccessible'],
        AutoMinorVersionUpgrade=original_rds['AutoMinorVersionUpgrade'],
        LicenseModel='string',
        DBName=original_rds['DBInstanceIdentifier'],
        Engine=original_rds['Engine'],
        Iops=original_rds['Iops'],
        OptionGroupName='string',
        Tags=[
            {
                'Key': 'string',
                'Value': 'string'
            },
        ],
        StorageType='string',
        TdeCredentialArn='string',
        TdeCredentialPassword='string',
        VpcSecurityGroupIds=[
            'string',
        ],
        Domain='string',
        CopyTagsToSnapshot=True|False,
        DomainIAMRoleName='string',
        EnableIAMDatabaseAuthentication=True|False,
        EnableCloudwatchLogsExports=[
            'string',
        ],
        ProcessorFeatures=[
            {
                'Name': 'string',
                'Value': 'string'
            },
        ],
        UseDefaultProcessorFeatures=True|False,
        DBParameterGroupName='string',
        DeletionProtection=True|False
    )


def main ():

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    rds_name = sys.argv[1]
    
    snapshot = get_latest_snapshot(rds_name)
    
    print ("Snapshot description")
    print(json.dumps(snapshot, indent=4, sort_keys=True, default=str))

    print(json.dumps(modify_rds (rds_name), indent=4, sort_keys=True, default=str))

    original_rds = rds.describe_db_instances(
        DBInstanceIdentifier=rds_name
    )['DBInstances'][0]

    print ("RDS description")
    print(json.dumps(original_rds, indent=4, sort_keys=True, default=str))
    
if (__name__ == '__main__'):
    main()
