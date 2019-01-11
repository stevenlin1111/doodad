import copy
import pickle
import sys
import json
import time

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
    launch_log_bucket = sys.argv[1]
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(launch_log_bucket)
    import googleapiclient.discovery
    compute = googleapiclient.discovery.build('compute', 'v1')


    while True:
        for blob in copy.copy(bucket.list_blobs(prefix='preempted/')):
            checkpoint_info = blob.download_as_string().decode("utf-8").splitlines()
            instance_name, checkpoint_commands = checkpoint_info[0], checkpoint_info[1:]
            launch_config_path = \
                    'doodad/mount/launch_config/{instance_name}.pkl'.format(
                        instance_name=instance_name
                    )
            launch_config = pickle.loads(
                bucket.get_blob(launch_config_path).download_as_string()
            )

            updated_launch_config = update_launch_config(launch_config, checkpoint_commands)
            compute.instances().insert(**updated_launch_config).execute()

            # update launch_config file with retry and preemption changes
            launch_config_filename = 'doodad/mount/launch_config/{instance_name}.pkl'.format(
                instance_name=instance_name
            )
            upload_file_to_gcp_storage(
                bucket_name=launch_log_bucket,
                file_contents=pickle.dumps(updated_launch_config),
                remote_filename=launch_config_filename,
            )
            # mark the preemption as handled
            blob.delete()
        time.sleep(300)
