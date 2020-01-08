# Snapshotter #

This service talks to exporter services and stores the result in s3. 

Environment variables required for running this in development mode:

  * `AWS_ACCESS_KEY_ID`
  * `AWS_SECRET_ACCESS_KEY`
  * `AWS_REGION`
  * `AWS_BUCKET`

The first three are the standard AWS environment variables; the last
names the bucket that the snapshotter will use.

# Deployment

There is an existing [Jenkins job](https://jenkins-build.socrata.com/job/snapshotter/) for the snapshotter. The job will build the application via sbt, build the docker image, and it will push the docker image to each environment's (staging/rc/prod/fedramp) docker ECR (Elastic Container Registry).

Once the Jenkins job completes successfully, you must manually deploy to each environment via [apps-marathon](https://github.com/socrata/apps-marathon#deployments).
