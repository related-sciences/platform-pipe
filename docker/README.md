# Docker Instructions

The containers included are extensions of those defined in https://github.com/gettyimages/docker-spark.

The "Client" (aka ot-client) container is intended to be used for running Spark pipelines and includes Debian Stretch, Spark 2.4.4, and Almond 0.9.0.

The "Import" (aka ot-import) container is used to export Elasticsearch (ES) indexes from the [docker-compose infrastructure in data_pipeline](https://github.com/opentargets/data_pipeline/blob/master/docker-compose.yml).  It can also be helpful for exploratory queryies against ES directly.

## Client

To build ```ot-client```:

```
cd $REPOS/platform-pipe/docker/ot-client
docker build --build-arg USERNAME=$USER --build-arg USERID=$(id -u) -t ot-client .
```

To run:

```
# On host with live data:
docker run --user $(id -u):$(id -g) --rm -ti \
-v $HOME/repos/rs/platform-pipe:/home/$USER/repos/platform-pipe \
-v $HOME/repos/ot/data_pipeline:/home/$USER/repos/data_pipeline \
-v /data/disk1/ot/dev:/home/$USER/data/ot \
-v $HOME/.ivy2:/home/$USER/.ivy2 \
-p 8888:8888 -p 4040:4040 -p 8080:8080 \
ot-client

# On local dev host:
docker run --user $(id -u):$(id -g) --rm -ti \
-v $HOME/repos/rs/platform-pipe:/home/$USER/repos/platform-pipe \
-v $HOME/repos/ot/data_pipeline:/home/$USER/repos/data_pipeline \
-v $HOME/data/ot:/home/$USER/data/ot \
-e PROJECT_JAR_PATH=/home/$USER/repos/platform-pipe/target/scala-2.12/platform-pipe.jar \
-p 8888:8888 -p 4040:4040 -p 8080:8080 \
ot-client
```

## Import

Build container used to run data_pipeline commands (for ES extracts):

```
cd $REPOS/platform-pipe/docker/ot-import
docker build --build-arg USERNAME=$USER --build-arg USERID=$(id -u) --build-arg DATA_PIPELINE_DIR=/home/$USER/repos/data_pipeline -t ot-import .
```

To run:

```
docker run --user $(id -u):$(id -g) --rm -ti \
--network data_pipeline_default \
-v $HOME/repos/ot/data_pipeline:/home/$USER/repos/data_pipeline \
-v $HOME/repos/rs/platform-pipe:/home/$USER/repos/platform-pipe \
-v /data/disk1/ot/dev:/home/$USER/data/ot \
-p 8889:8888 \
ot-import
```