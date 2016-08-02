# Snapshotter #

This service talks to exporter services and stores the result in s3. 

Environment variables required for running this in development mode:

  * `AWS_ACCESS_KEY_ID`
  * `AWS_SECRET_ACCESS_KEY`
  * `AWS_REGION`
  * `AWS_BUCKET`

The first three are the standard AWS environment variables; the last
names the bucket that the snapshotter will use.
