import copy
import pickle
import sys
import json
import time
import logging

from google.cloud import storage
from doodad.gcp.gcp_util import upload_file_to_gcp_storage

def get_metadata_value(metadata, key):
    for pair in metadata['items']:
        if pair['key'] == key:
            return pair['value']
    raise KeyError

def update_metadata_value(metadata, key, value, create=False):
    for pair in metadata['items']:
        if pair['key'] == key:
            pair['value'] = value
            return
    if not create:
        raise KeyError
    metadata['items'].append(
        {'key': key, 'value': value}
    )

def update_launch_config(launch_config, checkpoint_commands):
    # Weird google config format
    metadata = launch_config['body']['metadata']
    max_retries = get_metadata_value(metadata, 'max_retries')
    retries_so_far = get_metadata_value(metadata, 'retry')

    if retries_so_far == max_retries:
        launch_config['body']['scheduling']['preemptible'] = False
    else:
        update_metadata_value(metadata, 'retry', retries_so_far + 1)
    update_metadata_value(
        metadata,
        'checkpoint_commands',
        json.dumps(checkpoint_commands),
        create=True
    )
    return launch_config


if __name__ == "__main__":
    preemption_bucket = sys.argv[1]
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(preemption_bucket)
    import googleapiclient.discovery
    compute = googleapiclient.discovery.build('compute', 'v1')
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler('gcp_restart.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    while True:
        for blob in copy.copy(bucket.list_blobs(prefix='preempted/')):
            failure = False
            checkpoint_info = blob.download_as_string().decode("utf-8").splitlines()
            instance_name = checkpoint_info[0]
            checkpoint_commands = checkpoint_info[1:]
            log_msg = "Restarting {instance_name}".format(
                instance_name=instance_name
            )
            logging.info(log_msg)

            launch_config_path = \
                    'doodad/mount/launch_config/{instance_name}.pkl'.format(
                        instance_name=instance_name
                    )
            launch_config = pickle.loads(
                bucket.get_blob(launch_config_path).download_as_string()
            )

            updated_launch_config = update_launch_config(
                launch_config,
                checkpoint_commands
            )
            try:
                compute.instances().insert(**updated_launch_config).execute()
            except Exception as e:
                failure = True
                print(e)
                log_msg = "Failed to relaunch {instance_name}. Trying again later".format(
                    instance_name=instance_name
                )
                logging.warning(log_msg)


            # update launch_config file with retry and preemption changes
            # upload_file_to_gcp_storage automatically prepends with doodad/mount
            launch_config_filename = 'launch_config/{instance_name}.pkl'.format(
                instance_name=instance_name
            )
            upload_file_to_gcp_storage(
                bucket_name=preemption_bucket,
                file_contents=pickle.dumps(updated_launch_config),
                remote_filename=launch_config_filename,
                check_exists=False,
            )
            # mark the preemption as handled
            if not failure:
                blob.delete()
                log_msg = "Successfully relaunched {instance_name}".format(
                    instance_name=instance_name
                )
                logging.info(log_msg)
        time.sleep(10)
