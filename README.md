# Athena-Auto-Partitioning
This script will auto partition S3 bucket and create glue partitions on the fly.

Currently, Athena requires S3 buckets to be partitoned before Glue can create partitions for Athena. This script when attached to Firehose-Lambda transform, will handle all the churn and you can use Athena right away.
