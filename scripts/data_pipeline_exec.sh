# Data pipeline (https://github.com/opentargets/data_pipeline) script for loading ES indexes
# To execute, on non-container host:
# cd ~/repos/ot/data_pipeline
# docker-compose build # This will move custom mrdata configs into container
# bash ~/repos/rs/platform-pipe/scripts/data_pipeline_exec.sh

# Original data configuration:
# DCFG=https://storage.googleapis.com/open-targets-data-releases/19.09/input/mrtarget.data.19.09.yml
DCFG=mrtarget.data.19.11.yml  # Replace w/ local conf to ignore EuropePMC
CMD="docker-compose run mrtarget --data-config=$DCFG"
# Each of these should only take 1-10 minutes
#$CMD --rea
#$CMD --hpa
#$CMD --eco
#$CMD --efo
#$CMD --gen
#$CMD --val
$CMD --as