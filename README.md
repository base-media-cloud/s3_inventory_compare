# S3 inventory Compare

A python script to compare 2 x AWS S3 buckets using the inventory files created through the AWS inventory configuration.
The code can only compare the size using byte,
The previous code used etag as a checksum - this isn't necessarily the same for the source and destination bucket if any of the following is done:
The original object was uploaded using multipart upload (common for large files)
The copy operation itself uses multipart copy, which might happen automatically for large files (over 5 GB), or is explicitly done in parts.
The metadata is changed during the copy.
The object is encrypted (e.g., with SSE-KMS) — encryption can affect ETag calculation.

# Compare single inventory files
python s3_inventory_compare.py \
    --bucket1 my-source-bucket \
    --bucket2 my-dest-bucket \
    --inventory1 inventory/data.csv.gz \
    --inventory2 inventory/data.csv.gz

# Compare using manifest files
python s3_inventory_compare.py \
    --bucket1 my-source-bucket \
    --bucket2 my-dest-bucket \
    --inventory1 inventory/manifest.json \
    --inventory2 inventory/manifest.json \
    --use-manifest

# Use specific AWS profile
python s3_inventory_compare.py \
    --bucket1 bucket1 \
    --bucket2 bucket2 \
    --inventory1 inventory.csv \
    --inventory2 inventory.csv \
    --profile my-aws-profile
