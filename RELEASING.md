# Releasing Guide for Kroxylicious Junit5 Extension

This document describes how to release this component.


## Release Pre-Requisites

In addition to the build pre-requistes:

- [`gh`](https://cli.github.com/) Github CLI (optional)

## Releasing this project

Firstly ensure that you have completed the [release requirements](https://github.com/kroxylicious/kroxylicious/blob/main/docs/Releasing.adoc#requirements).  Once that is done, use
the release script to actually perform the release and prepare main for the next development version.

```shell
./scripts/release.sh -k <YOUR_KEY_SHORT_ID> -v <RELEASE_VERSION>
```


