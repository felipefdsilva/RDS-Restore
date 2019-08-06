import boto3
import json
import sys

class RDSCopyException (Exception):
    pass

#Retrieves the rds snapshot list and return the latest one
def get_latest_snapshot (rds_name):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    snapshot_list = rds.describe_db_snapshots(
        DBInstanceIdentifier=rds_name
    )['DBSnapshots']

    if (len(snapshot_list) == 0):
        raise RDSCopyException ("No snapshot for this db instance")

    snapshot_list = sorted(snapshot_list, key=lambda snapshot: snapshot['InstanceCreateTime'])

    return snapshot_list[-1]

#Returns the description of a rds instance
def get_rds_description (rds_name):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    return rds.describe_db_instances(DBInstanceIdentifier=rds_name)['DBInstances'][0]

#Return the tags of a RDS instance
def get_tags (rds_arn):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    tags = rds.list_tags_for_resource (ResourceName=rds_arn)['TagList']

    return tags

#Deletes a RDS
def delete_rds (rds_name):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    deleted_rds = rds.delete_db_instance (
        DBInstanceIdentifier=rds_name,
        SkipFinalSnapshot=True,
        DeleteAutomatedBackups=False
    )
    rds.get_waiter('db_instance_deleted').wait(DBInstanceIdentifier=rds_name)


#Creates a RDS from the latest snapshot
def create_rds (snapshot_name, rds_description):

    rds = boto3.Session(profile_name='poc', region_name='us-east-1').client('rds')

    vpc_sec_groups_ids = []

    for sg in rds_description['VpcSecurityGroups']:
        vpc_sec_groups_ids.append(sg['VpcSecurityGroupId'])

    new_rds = rds.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=rds_description['DBInstanceIdentifier'],
        DBSnapshotIdentifier=snapshot_name,
        DBInstanceClass=rds_description['DBInstanceClass'],
        AvailabilityZone=rds_description['AvailabilityZone'],
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
        Tags=get_tags(rds_description['DBInstanceArn'])
    )
    return new_rds

def main ():
    rds_name = sys.argv[1]

    try:
        snapshot = get_latest_snapshot(rds_name)

    except RDSCopyException as e:
        print (e)
        exit(1)

    rds_description = get_rds_description(rds_name)

    delete_rds(rds_name)

    new_rds = create_rds(snapshot['DBSnapshotIdentifier'], rds_description)

    print ("New RDS")
    print (json.dumps(new_rds, default=str, indent=4, sort_keys=True))

if (__name__ == '__main__'):
    main()
