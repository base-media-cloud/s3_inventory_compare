# S3 inventory Compare

A python script to compare 2 x AWS S3 buckets using the inventory files created through the AWS inventory configuration.
The code can only compare the size using byte,
The previous code used etag as a checksum - this isn't necessarily the same for the source and destination bucket if any of the following is done:
The original object was uploaded using multipart upload (common for large files)
The copy operation itself uses multipart copy, which might happen automatically for large files (over 5 GB), or is explicitly done in parts.
The metadata is changed during the copy.
The object is encrypted (e.g., with SSE-KMS) — encryption can affect ETag calculation.
