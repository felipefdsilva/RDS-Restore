import boto3
import sys
import json

client = boto3.client('rds')

snapshot_metadata = client.describe_db_snapshots(
    DBSnapshotIdentifier='my-teste-automation-snap01'
)['DBSnapshots'][0]

print ("Snapshot description")
print(json.dumps(snapshot_metadata, indent=4, sort_keys=True, default=str))

source_rds_metadata = client.describe_db_instances(
    DBInstanceIdentifier='mytesteautomation-modified'
)['DBInstances'][0]

print ("RDS description")
print(json.dumps(source_rds_metadata, indent=4, sort_keys=True, default=str))
"""
modify_rds = client.modify_db_instance (
    DBInstanceIdentifier=sys.argv[4],
    NewDBInstanceIdentifier=sys.argv[5]
)

print(json.dumps(modify_rds, indent=4, sort_keys=True, default=str))
"""
