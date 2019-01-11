import os

from doodad.utils import hash_file, call_and_wait, CommandBuilder, REPO_DIR

GCP_STARTUP_SCRIPT_PATH = os.path.join(REPO_DIR, "scripts/gcp/gcp_startup_script.sh")
GCP_SHUTDOWN_SCRIPT_PATH = os.path.join(REPO_DIR, "scripts/gcp/gcp_shutdown_script.sh")

def upload_file_to_gcp_storage(
    bucket_name,
    file_name=None,
    file_contents=None,
    remote_filename=None,
    dry=False,
    check_exists=True,
):
    from google.cloud import storage
    assert file_name or file_contents, "must specify local filepath or contents"
    assert not (file_name and file_contents)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    if remote_filename is None:
        assert file_name
        remote_filename = os.path.basename(file_name)
    remote_path = 'doodad/mount/' + remote_filename
    blob = bucket.blob(remote_path)
    if check_exists and blob.exists(storage_client):
        print("{remote_path} already exists".format(remote_path=remote_path))
        return remote_path

    if file_name:
        blob.upload_from_filename(file_name)
    elif file_contents:
        blob.upload_from_string(file_contents)
    return remote_path

def get_machine_type(zone, instance_type):
    return "zones/{zone}/machineTypes/{instance_type}".format(
            zone=zone,
            instance_type=instance_type,
    )

def get_gpu_type(project, zone, gpu_model):
    """
    Check the available gpu models for each zone
    https://cloud.google.com/compute/docs/gpus/
    """
    assert gpu_model in [
        'nvidia-tesla-p4',
        'nvidia-tesla-k80',
        'nvidia-tesla-v100',
        'nvidia-tesla-p100'
    ]

    return (
        "https://www.googleapis.com/compute/v1/"
        "projects/{project}/zones/{zone}/acceleratorTypes/{gpu_model}".format(
            project=project,
            zone=zone,
            gpu_model=gpu_model
        )
    )

