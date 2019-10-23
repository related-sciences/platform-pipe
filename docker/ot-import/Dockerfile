# Lift primarily from OT data pipeline docker build: https://github.com/opentargets/data_pipeline/blob/master/Dockerfile
FROM python:3.7-slim
ARG DATA_PIPELINE_DIR
ARG USERNAME
ARG USERID

# Match client user
RUN useradd -ms /bin/bash -u $USERID $USERNAME
RUN usermod -aG sudo $USERNAME
RUN echo "$USERNAME     ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    time \
    htop

COPY ./requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r /usr/src/app/requirements.txt

ENV PYTHONPATH="$DATA_PIPELINE_DIR:$PYTHONPATH"

CMD /bin/bash -c "jupyter lab --allow-root --ip=0.0.0.0"