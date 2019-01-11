#!/bin/bash
query_metadata() {
    attribute_name=$1
    curl http://metadata/computeMetadata/v1/instance/attributes/$attribute_name -H "Metadata-Flavor: Google"
}

{
bucket_name=$(query_metadata bucket_name)
gcp_mounts=$(query_metadata gcp_mounts)
instance_name=$(curl http://metadata/computeMetadata/v1/instance/name -H "Metadata-Flavor: Google")
num_gcp_mounts=$(jq length <<< $gcp_mounts)

preempted=$(curl "http://metadata.google.internal/computeMetadata/v1/instance/preempted" -H "Metadata-Flavor: Google")
echo "preempted:" $preempted
if [ "$preempted" = "TRUE" ]; then

    echo $instance_name > checkpoint_info
    for ((i=0;i<$num_gcp_mounts;i++)); do
        gcp_mount_info=$(jq .[$i] <<< $gcp_mounts)
        local_path=$(jq .[0] <<< $gcp_mount_info | tr -d '"')
        gcp_bucket_path=$(jq .[1] <<< $gcp_mount_info | tr -d '"')
        # checkpoint dirs
        ls $local_path > /tmp/checkpoint_ls
        while read p; do
            local_checkpoint_path=$(echo $local_path/$p | sed s#//*#/#g)
            remote_checkpoint_path=gs://"$(echo $bucket_name/$gcp_bucket_path/"$p" | sed s#//*#/#g)"
            echo "mkdir -p $local_checkpoint_path && gsutil -m rsync -r $remote_checkpoint_path $local_checkpoint_path" >> checkpoint_info
        done < /tmp/checkpoint_ls
    done
    cat checkpoint_info

    preemption_bucket=$(query_metadata preemption_bucket)
    gsutil cp checkpoint_info gs://$preemption_bucket/preempted/$instance_name
fi
gsutil cp /home/ubuntu/user_data.log gs://$bucket_name/$gcp_bucket_path/${instance_name}_stdout.log

for ((i=0;i<$num_gcp_mounts;i++)); do
    gcp_mount_info=$(jq .[$i] <<< $gcp_mounts)
    # assume gcp_mount_info is a (local_path, bucket_path, include_string, periodic_sync_interval) tuple
    local_path=$(jq .[0] <<< $gcp_mount_info | tr -d '"')
    gcp_bucket_path=$(jq .[1] <<< $gcp_mount_info | tr -d '"')
    gsutil -m rsync -r $local_path gs://$bucket_name/$gcp_bucket_path
done

} >> /home/ubuntu/terminate.log 2>&1
gsutil cp /home/ubuntu/terminate.log gs://$bucket_name/$gcp_bucket_path/${instance_name}_terminate.log
zone=$(curl http://metadata/computeMetadata/v1/instance/zone -H "Metadata-Flavor: Google")
zone="${zone##*/}"
gcloud compute instances delete $instance_name --zone $zone --quiet
