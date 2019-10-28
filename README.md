# ot-scoring
Dynamic scoring infrastructure for Open Targets


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