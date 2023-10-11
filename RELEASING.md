# Releasing Guide for Kroxylicious Junit5 Extension

This document describes how to release this component.


## Release Pre-Requisites

In addition to the build pre-requistes:

- [`gh`](https://cli.github.com/) Github CLI (optional)

## Releasing this project

Firstly ensure that you have completed the [release requirements](https://github.com/kroxylicious/kroxylicious/blob/main/RELEASING.md#requirements).  Once that is done, use
the release script to actually perform the release and prepare main for the next development version.

```shell
./scripts/release.sh -k <YOUR_KEY_SHORT_ID> -v <RELEASE_VERSION>
```

where `<YOUR_KEY_SHORT_ID>` is the short id of your PGP key
and `<RELEASE_VERSION>` is a release number such as `0.6.0`.

Once the release is completed, use the [Nexus UI](https://s01.oss.sonatype.org/) to close the staging repository, then release it. That will push the Maven artefacts available
on Maven Central.  The artefacts will take a while to reach all the Maven Central mirrors.



