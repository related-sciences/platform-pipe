[![Build Status](https://travis-ci.org/related-sciences/platform-pipe.svg?branch=master)](https://travis-ci.org/related-sciences/platform-pipe)
[![codecov](https://codecov.io/gh/related-sciences/platform-pipe/branch/master/graph/badge.svg)](https://codecov.io/gh/related-sciences/platform-pipe)

# Open Targets Platform Pipeline

This project is intended to validate, normalize and score [evidence](https://docs.targetvalidation.org/getting-started/evidence) for Open Targets (OT) Platform, and is structured as a counterpart to [genetics-pipe](https://github.com/opentargets/genetics-pipe).  The pipeline here replaces much of what was originally maintained in [data_pipeline](https://github.com/opentargets/data_pipeline), moving it to a more concise and efficient Scala/Spark framework.  It is also intended to make scoring much more configurable for any clients that may wish to tune scoring coefficients for their own purposes.

There are two primary steps in this pipeline:

1. **Evidence Preparation** 
  - See [EvidencePreparationPipeline.scala](src/main/scala/com/relatedsciences/opentargets/etl/pipeline/EvidencePreparationPipeline.scala) for implementation details.
  - This phase of the pipeline will validate and normalize the json evidence strings associated with OT data sources.  These files are generated in large part by [platform-input-support](https://github.com/opentargets/platform-input-support) and are primarily stored in Google Storage (GS).  Links to files for each source are maintained in a [pipeline-configuration](https://docs.targetvalidation.org/technical-pipeline/technical-notes#pipeline-configuration-file) file that is updated with each new release.
  - At a high level, this step requires Elasticsearch index dumps (for gene/disease metadata) as well as GS files and produces a single parquet dataset ([schema](docs/evidence_schema.txt)).
  - Notable operations performed in this phase include:
    - Validation of evidence strings against the [OT Evidence Schema](https://github.com/opentargets/json_schema/blob/master/opentargets.json)
    - Normalization of UniProt and non-reference (i.e. genes defined against non-reference assemblies, typically in highly polymorphic regions) targets
    - Evidence code aggregation and static scoring; i.e. some scores are defined purely based on evidence codes and need to overriden in this phase (see [here](https://github.com/related-sciences/platform-pipe/blob/85e702956e8cca21a1f0050390801817d02dfed2/src/main/scala/com/relatedsciences/opentargets/etl/pipeline/EvidencePreparationPipeline.scala#L304) for details)
    - Target and disease validation based on ensembl and EFO accessions, respectively
    - Aggregation of all filtering and nearly all mutation operations into ancillary datasets that can be used to trace why records were lost or altered; see this [Validation Error Report](https://nbviewer.jupyter.org/github/related-sciences/ot-scoring/blob/master/notebooks/pipeline/error-summary-analysis.ipynb#Record-Invalidation-Cause-Frequency-by-Source) for an example summary.
2. **Evidence Scoring**    
  - See [ScoringPreparationPipeline.scala](https://github.com/related-sciences/platform-pipe/blob/master/src/main/scala/com/relatedsciences/opentargets/etl/pipeline/ScoringPreparationPipeline.scala) and [ScoringCalculationPipeline.scala](https://github.com/related-sciences/platform-pipe/blob/master/src/main/scala/com/relatedsciences/opentargets/etl/pipeline/ScoringCalculationPipeline.scala) for implementation details
  - This phase of the pipeline will score evidence created by the preparation step.  While this will likely expand in the future to include more of the parameters used in scoring, the data source weights, at least, are configurable as shown here in [application.conf](https://github.com/related-sciences/platform-pipe/blob/85e702956e8cca21a1f0050390801817d02dfed2/src/main/resources/application.conf#L42)
  - Most of the trickier details related to per-source handling of evidence can be found in [Scoring.scala](https://github.com/related-sciences/platform-pipe/blob/master/src/main/scala/com/relatedsciences/opentargets/etl/scoring/Scoring.scala)

## Validation

Both evidence preparation and scoring can be validated against output from the original [data_pipeline](https://github.com/opentargets/data_pipeline) implementation in [evidence-prep-validation.ipynb](https://github.com/related-sciences/platform-pipe/blob/master/notebooks/pipeline/evidence-prep-validation.ipynb) and [scoring-validation.ipynb](https://github.com/related-sciences/platform-pipe/blob/master/notebooks/pipeline/scoring-validation.ipynb), respectively.  These notebooks contain checks for target/disease presence and field equality across all data sources.  There were several issues encountered that are mentioned in the notebooks but all data was found equivalent outside of the [issues](https://github.com/opentargets/platform/issues?utf8=%E2%9C%93&q=is%3Aissue+author%3Aeric-czech) raised on github.

There are also tests [like this one](https://github.com/related-sciences/platform-pipe/blob/master/src/test/scala/com/relatedsciences/opentargets/etl/PipelineSuite.scala) intended to preserve a subset of these checks as part of the CI build.

## Configuration

## Prerequisities

*Note: All of the below are specific to the 19.11 OT release*

This project will expect two primary sources of input information and while the process outlined below is a bit cumbersome as of now, we expect to improve it after the scope of the project is solidified:

1. Metadata index extracts from Elasticsearch 
  - The ```gene```, ```eco```, and ```efo``` indexes are currently required
  - These can be created one of the following two ways:
    1. By setting up and running data_pipeline yourself
      - See [data_pipeline#overview](https://github.com/opentargets/data_pipeline#overview) for general instructions
      - See [scripts/data_pipeline_exec.sh](scripts/data_pipeline_exec.sh) for a script that will run the necessary steps above (only everything up to the "association" step is needed)
      - See [scripts/data_pipeline_extract.py](scripts/data_pipeline_extract.py) for instructions on how to create the json dumps from ES
    2. By downloading and decompressing the pre-prepared dumps at ```https://storage.googleapis.com/platform-pipe/extract/{gene,eco,efo}.json.gz```
      - An example script to do this is:
        ```bash
        mkdir -p $DATA_DIR/extract; cd $DATA_DIR/extract 
        for index in gene eco efo; do
        mkdir ${index}.json
        wget -P ${index}.json https://storage.googleapis.com/platform-pipe/extract/${index}.json.gz
        gzip -d ${index}.json/${index}.json.gz 
        done
        ```
2. Evidence files 
    - See [download_evidence_files.sh] for a script that will download this information
    - These files will collectively occupy about 23G of space (17G of which is from a single source, europepmc, so developers may find it convenient to remove or subset this file for testing)

## Build and Execution

To build the project, run:

```bash
sbt clean assembly
# or for no tests: sbt "set test in assembly := {}" clean assembly
```

This will produce ```target/scala-2.12/platform-pipe.jar``` which can be deployed or run locally.

To execute all pipeline steps, run the following using the ```ot-client``` docker (see [docker/README.md](docker/README.md)) container provided or your own cluster:

```bash
APP=$REPOS/platform-pipe/target/scala-2.12/platform-pipe.jar

for cmd in prepare-evidence prepare-scores calculate-scores; do
echo "Running command: $cmd"
# Note: High driver-memory below not necesssary on a cluster -- this is for runs in local mode
/usr/spark-2.4.4/bin/spark-submit \
--driver-memory 64g \
--class com.relatedsciences.opentargets.etl.Main $APP $cmd  \
--config $HOME/repos/platform-pipe/src/main/resources/application.conf
done
```

## Developer Notes

### Ancillary Script Execution
 
Evidence test data generation script:

```
# Run script on ot-client container
/usr/spark-2.4.4/bin/spark-shell --driver-memory 12g \
--jars $HOME/data/ot/apps/platform-pipe.jar \
-i $HOME/data/ot/apps/scripts/create_evidence_test_datasets.sc \
--conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=1 \
--conf spark.driver.args="\
extractDir=$HOME/data/ot/extract,\
testInputDir=$HOME/repos/platform-pipe/src/test/resources/pipeline_test/input,\
testExpectedDir=$HOME/repos/platform-pipe/src/test/resources/pipeline_test/expected"
```

### Scalafmt Installation

A pre-commit hook to run [scalafmt](https://scalameta.org/scalafmt/) is recommended for 
this repo though installation of scalafmt is left to developers. The [Installation Guide](https://scalameta.org/scalafmt/docs/installation.html)
has simple instructions, and the process used for Ubuntu 18.04 was:

```bash
cd /tmp/  
curl -Lo coursier https://git.io/coursier-cli &&
    chmod +x coursier &&
    ./coursier --help
sudo ./coursier bootstrap org.scalameta:scalafmt-cli_2.12:2.2.1 \
  -r sonatype:snapshots \
  -o /usr/local/bin/scalafmt --standalone --main org.scalafmt.cli.Cli
scalafmt --version # "scalafmt 2.2.1" at TOW
```

The pre-commit hook can then be installed using:

```bash
cd $REPOS/platform-pipe
chmod +x hooks/pre-commit.scalafmt 
ln -s $PWD/hooks/pre-commit.scalafmt .git/hooks/pre-commit
```

After this, every commit will trigger scalafmt to run and ```--no-verify``` can be 
used to ignore that step if absolutely necessary.
