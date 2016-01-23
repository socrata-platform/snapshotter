## Building ##

### Pre-Requisites ###
Both
[`sbt`](http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Linux.html)
and a
[JDK](http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html)
(oracle-jdk-8 recommended)
must be installed to build.

To build the image, run:
```
cd .. # Change to project root.
sbt assembly
cp target/scala-2.11/snapshotter-assembly-0.0.1-SNAPSHOT.jar docker/snapshotter-assembly.jar
docker build -t snapshotter docker
```

Or, if you want to replace old versions:
```
cd .. # Change to project root.
docker build --rm -t snapshotter docker
```

## Running ##
```
docker run -p 6800:6800 -e ZOOKEEPER_ENSEMBLE=<HOST> -e AWS_BUCKET=<BUCKET> -e AWS_ACCESS_KEY_ID=<ACCESS_KEY_ID> -e AWS_SECRET_KEY=<ACCESS_SECRET_ID> -d snapshotter
```

For example:
```
docker run -p 6800:6800 -e \
ZOOKEEPER_ENSEMBLE='["10.110.35.228","10.110.38.74","10.110.40.61"]' \
-e AWS_BUCKET='dataset-snapshot-us-west-2-staging' \
-e AWS_ACCESS_KEY_ID='AKIAJ5MAZW76KVWZD3FA' \
-e AWS_SECRET_KEY='itsasecret' \
-d snapshotter
```

## Required Environment Variables ##
* `ZOOKEEPER_ENSEMBLE` - A list of hostnames and ports of zookeeper instances. eg: ["10.0.0.1:2181", "10.0.0.2:2818"]
* `AWS_BUCKET` - The bucket holding our snapshots
* `AWS_ACCESS_KEY_ID` - The access key to the bucket
* `AWS_SECRET_KEY` - The secret key associated with the access key

## Optional Runtime Variables ##
See the [DockerFile](Dockerfile) for defaults.

* `JAVA_XMX`                - Sets the JVM heap size.