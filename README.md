# OpenTargets Scoring

The purpose of this project is to adapt [data_pipeline](https://github.com/opentargets/data_pipeline) to a Scala/Spark
infrastructure so that target/disease association scoring can be both more efficient and configurable.

At present, this pipeline uses some of the schema validation and data sanitation present in [data_pipeline]([data_pipeline](https://github.com/opentargets/data_pipeline))
but then conducts all further scoring in a two-stage process.  The first generates a parquet dataset most amenable to
quickly scoring associations given a set of configurable weights for sources, data types, and individual resource 
score components (e.g. GWAS sample size, p-value, and gene to variant confidence).  The second combines this data
with a set of weights (possibly via RPC in the future) to compute scores across all data sources.  At present this 


## Developer Notes

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
cd $REPOS/ot-scoring
chmod +x hooks/pre-commit.scalafmt 
ln -s $PWD/hooks/pre-commit.scalafmt .git/hooks/pre-commit
```

After this, every commit will trigger scalafmt to run and ```--no-verify``` can be 
used to ignore that step if absolutely necessary.