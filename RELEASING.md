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

The script will push the Maven artefacts to a Nexus **staging repository** and it will open a PR that will commit the release to Git.

The next steps are:
1.  Validate the staged artefacts.
2.  If the staging artefacts are **good**, release the stage repository.  If they are not, **drop** the stage repository and start the release over.
3.  Merge the PR.

These are described below.

### Validating the staged artefacts

You can validate the staged artefacts by having a test application (we'll call it `T`) that uses kroxylicious-junit5-extension use the Maven artefacts by making
temporary (local) changes to its POM.

1. Find the staging repository in the [Nexus UI](https://s01.oss.sonatype.org/). It'll be named `iokroxylioustesting-nn`.
2. [Close](https://help.sonatype.com/repomanager2/staging-releases/managing-staging-repositories) the staging respository.  This gives the staging repository a publicly accessible URL.
3. Add a [`<repositories>`](https://maven.apache.org/pom.html#Repositories) that references the staging repository public url to `T`'s POM.
4. Update `T`'s kroxylicious-junit5-extension dependency to refer to the `<RELEASE_VERSION>`.
5. Run `T` build/test cycle but use an alternative cache location to be sure artefacts are being fetched.  Check the build output, you'll see the
   kroxylicious-junit5-extension come from the staging location. 
```
MAVEN_OPTS="-Dmaven.repo.local=/tmp/repository" mvn verify
```
If the build passes, proceed to release otherwise the staging repository should be dropped.
The local changes made to `T`'s POM can be reverted. 

### Releasing the stage repository and merge the PR.

Use the [Release](https://help.sonatype.com/repomanager2/staging-releases/managing-staging-repositories) to release artefacts.  This makes the
artefacts public and they'll appear in Maven Central.  Note that propagation to Maven Central's mirrors can take time (hours).

Use the Github workflow to merge the PR that commits the release to Git.

### Dropping the stage repository

If the validation step fails and the maven artefacts are deemed bad, drop the staging repository.

Use the [Drop](https://help.sonatype.com/repomanager2/staging-releases/managing-staging-repositories) to drop the stage repository.

In this case the PR should be closed without merging.

Once the issue is resolved, repeat the release, bumping the release micro by 1.





