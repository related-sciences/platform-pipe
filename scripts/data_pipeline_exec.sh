# Data pipeline (https://github.com/opentargets/data_pipeline) script for loading ES indexes
# cd $REPOS/data_pipeline

# Original data configuration:
# DCFG=https://storage.googleapis.com/open-targets-data-releases/19.09/input/mrtarget.data.19.09.yml
DCFG=mrtarget.data.19.10.yml  # Replace w/ local conf to ignore EuropePMC
CMD="docker-compose run mrtarget --data-config=mrtarget.data.19.10.yml"
# Each of these should only take 1-10 minutes
#$CMD --rea
#$CMD --hpa
#$CMD --eco
#$CMD --efo
#$CMD --gen
#$CMD --val
#$CMD --as