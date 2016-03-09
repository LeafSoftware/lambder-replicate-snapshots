import boto3
import logging
import pprint
import os
import os.path
import json
from datetime import datetime

class Replicator:

  REPLICATE_TAG = "LambderReplicate"
  BACKUP_TAG = "LambderBackup"

  def __init__(self):
    logging.basicConfig()
    self.logger = logging.getLogger()

    # set location of config file
    script_dir = os.path.dirname(__file__)
    config_file = script_dir + '/config.json'

    # if there is a config file in place, load it in. if not, bail.
    if not os.path.isfile(config_file):
      self.logger.error(config_file + " does not exist")
      exit(1)
    else:
      config_data=open(config_file).read()
      config_json = json.loads(config_data)
      self.AWS_SOURCE_REGION=config_json['AWS_SOURCE_REGION']
      self.AWS_DEST_REGION=config_json['AWS_DEST_REGION']

    self.ec2_source = boto3.resource('ec2', region_name=self.AWS_SOURCE_REGION)
    self.ec2_dest = boto3.resource('ec2', region_name=self.AWS_DEST_REGION)

  def get_source_snapshots(self):
    filters = [{'Name':'tag-key', 'Values': [self.REPLICATE_TAG]}]
    snapshots = self.ec2_source.snapshots.filter(Filters=filters)
    return snapshots

  def get_dest_snapshots(self,snapid,backupname):
    filters = [{'Name':'description', 'Values': [self.AWS_SOURCE_REGION+'_'+snapid+'_'+backupname]}]
    snapshots = self.ec2_dest.snapshots.filter(Filters=filters)
    return snapshots

  # Takes an snapshot or volume, returns the backup source
  def get_backup_source(self, resource):
    tags = filter(lambda x: x['Key'] == self.BACKUP_TAG, resource.tags)

    if len(tags) < 1:
      return None

    return tags[0]['Value']

  def copy_snapshot(self,snapshot):
    sourcesnapid=snapshot.snapshot_id
    sourcebackupname=self.get_backup_source(snapshot)
    self.logger.info("Looking for existing replicas of snapshot {0}".format(sourcesnapid))
    dest_snapshots=self.get_dest_snapshots(sourcesnapid,sourcebackupname)
    dest_snapshot_count = len(list(dest_snapshots))
    if dest_snapshot_count != 0:
      self.logger.info("Replica found, no need to copy snapshot")
    else:
      self.logger.info("No replica found, copying snapshot {0}".format(sourcesnapid))
      sourcesnap = self.ec2_dest.Snapshot(sourcesnapid)
      dest_snap_description=self.AWS_SOURCE_REGION+'_'+sourcesnapid+'_'+sourcebackupname
      copy_output=sourcesnap.copy(DryRun=False,SourceRegion=self.AWS_SOURCE_REGION,SourceSnapshotId=sourcesnapid,Description=dest_snap_description)
      destsnapid=copy_output['SnapshotId']
      destsnap = self.ec2_dest.Snapshot(destsnapid)
      destsnap.create_tags(Tags=[
        {'Key': self.REPLICATE_TAG, 'Value': dest_snap_description},
        {'Key': self.BACKUP_TAG, 'Value': sourcebackupname}])

  def copy_snapshots(self,snapshots):
    for snapshot in snapshots:
      self.copy_snapshot(snapshot)

  def run(self):

    # replicate any snapshots that need to be replicated
    source_snapshots = self.get_source_snapshots()
    source_snapshot_count = len(list(source_snapshots))

    self.logger.info("Found {0} source snapshots".format(source_snapshot_count))

    self.copy_snapshots(source_snapshots)
