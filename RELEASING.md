# Releasing Guide for Kroxylicious Junit5 Extension

This document describes how to release this component.

The component is released using GitHub automation.

At a high level, the process is as follows:

1. The developer adds their PGP private key/passphrase as repository secrets
1. The workflow `stage_release` tags, builds/signs the release, and stages the release on a Nexus staging repository.
1. The stage release is verified using manual verification steps.
1. The workflow `deploy_release` releases from the staged repository to Maven Central.
1. The developer removes their PGP private key/passphrase as repository secrets.

## Pre-Requisites

You must be a member of the Kroxylicious organization and have access to [create 
secrets](https://github.com/kroxylicious/kroxylicious-junit5-extension/settings/secrets/actions) within the
kroxylicious-junit5-extension repository.  Create the following repository secrets.


| Secret                               | Description                                   |
|--------------------------------------|-----------------------------------------------|
| KROXYLICIOUS_RELEASE_GPG_PRIVATE_KEY | GPG private key for Kroxylicious (plain text) |
| KROXYLICIOUS_RELEASE_GPG_PASSPHRASE  | GPG passphrase                                |


## Release steps

Use the [Kroxylicious Team Developers](https://kroxylicious.slack.com/archives/C04V1K6EAKZ) Slack Channel to coordinate
the release with the other developers.  It is important no other work is merged to main during the release.

### Stage the Release

Run [stage_workflow](https://github.com/kroxylicious/kroxylicious-junit5-extension/actions/workflows/stage_release.yml).
Set the `release-version` argument to the version being release e.g. `0.7.0`.

This will:

* raise single PR that will contain two commits:
  1. the first will version the artefacts at `release-version`.  A `release-version` tag will point at this commit.
  2. the second will re-open main for development, at the next snapshot.
* stage a release [Nexus UI](https://s01.oss.sonatype.org/). It'll be named `iokroxylioustesting-nn`.

### Verify the Release

You can validate the staged artefacts by having a test application (we'll call it `T`) that uses kroxylicious-junit5-extension use the Maven artefacts by making
temporary (local) changes to its POM.

1. Find the staging repository in the [Nexus UI](https://s01.oss.sonatype.org/). It'll be named `iokroxylioustesting-nn`.
2. Add a [`<repositories>`](https://maven.apache.org/pom.html#Repositories) that references the staging repository public url to `T`'s POM.
3. Update `T`'s kroxylicious-junit5-extension dependency to refer to the `<RELEASE_VERSION>`.
4. Run `T` build/test cycle but use an alternative cache location to be sure artefacts are being fetched.  Check the build output, you'll see the
   kroxylicious-junit5-extension come from the staging location.
```
MAVEN_OPTS="-Dmaven.repo.local=/tmp/repository" mvn verify
```
If the build passes, proceed with the next two steps.
The local changes made to `T`'s POM can be reverted.

### Deploy Release

Run [deploy_workflow](https://github.com/kroxylicious/kroxylicious-junit5-extension/actions/workflows/deploy_release.yml).
Set the `next-state` to `release` to publish the artefact. 

### Merge the PR

To complete the release, merge the PR.  Otherwise if the release 

### Failed Releases

If the release fails verification, use the [deploy_workflow](https://github.com/kroxylicious/kroxylicious-junit5-extension/actions/workflows/deploy_release.yml)
to `drop` the snapshot repository and close the PR.

The release tag needs to be removed manually:

```shell
git push --delete origin v0.7.0
```

### Remove PGP secrets

Remove your PGP secrets from the [repository secrets]((https://github.com/kroxylicious/kroxylicious-junit5-extension/settings/secrets/actions) .)



