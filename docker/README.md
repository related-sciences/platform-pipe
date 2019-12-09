# Docker Instructions

Spark Cluster: https://github.com/gettyimages/docker-spark

## Client

```
cd $REPOS/ot-scoring/docker/ot-client
docker build --build-arg USERNAME=$USER --build-arg USERID=$(id -u) -t ot-client .
```

```
docker run --user $(id -u):$(id -g) --rm -ti \
-v $HOME/repos/rs/ot-scoring:/home/$USER/repos/ot-scoring \
-v $HOME/repos/ot/data_pipeline:/home/$USER/repos/data_pipeline \
-v /data/disk1/ot/dev:/home/$USER/data/ot \
-p 8888:8888 -p 4040:4040 \
ot-client
```

## Import

Docker container used to run data_pipeline commands (for ES extracts):

```
cd $REPOS/ot-scoring/docker/ot-import
docker build --build-arg USERNAME=$USER --build-arg USERID=$(id -u) --build-arg DATA_PIPELINE_DIR=/home/$USER/repos/data_pipeline -t ot-import .
```

```
docker run --user $(id -u):$(id -g) --rm -ti \
--network data_pipeline_default \
-v $HOME/repos/ot/data_pipeline:/home/$USER/repos/data_pipeline \
-v $HOME/repos/rs/ot-scoring:/home/$USER/repos/ot-scoring \
-v /data/disk1/ot/dev:/home/$USER/data/ot \
-p 8889:8888 \
ot-import
```