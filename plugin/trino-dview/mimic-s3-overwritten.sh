#!/bin/bash

# Extract AWS access key ID from the credentials file
AWS_ACCESS_KEY=$(grep 'aws_access_key_id' ~/.aws/credentials | awk '{print $3}')

# Extract AWS secret access key from the credentials file
AWS_SECRET_KEY=$(grep 'aws_secret_access_key' ~/.aws/credentials | awk '{print $3}')

# Extract default region from the configuration file
AWS_REGION=$(grep 'region' ~/.aws/config | awk '{print $3}')


aws configure set aws_access_key_id "$AWS_ACCESS_KEY";
aws configure set aws_secret_access_key "$AWS_SECRET_KEY";
aws configure set default.region "$AWS_REGION";

# AWS S3 details
S3_FILE="s3://dview-nrt/tmp/fiber/snapshot/data/dview.analytics.metabase-ecombooks.task_history/task_history/dt=2021-12-18/"
LOCAL_FILE="tmp/"

NUM_LOOPS=1000  # Adjust the number of loops as needed

# shellcheck disable=SC2004
for ((i=1; i<=$NUM_LOOPS; i++)); do
    # Copy file from S3 to local disk
    aws s3 cp $S3_FILE $LOCAL_FILE
    # Delete file from S3
    aws s3 rm $S3_FILE

    # Sleep for x seconds (adjust as needed)
    sleep 1

    # Copy the file back to S3
    aws s3 cp $LOCAL_FILE $S3_FILE --recursive

    # Cleanup: Remove the local copy
#    rm -rf $LOCAL_FILE

    echo "Iteration $i completed."

    # Sleep between iterations if needed
    sleep 5  # Adjust as needed
done

echo "All iterations completed."
