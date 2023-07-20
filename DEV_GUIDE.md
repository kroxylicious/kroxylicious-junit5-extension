# Development Guide for Kroxylicious Junit5 Extension

This document gives a detailed breakdown of the various build processes and options for building the Kroxylicious Junit5 Extension from source.

## Build Prerequisites

- [JDK](https://openjdk.org/projects/jdk/17/) (version 17 and above) - Maven CLI
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`docker`](https://docs.docker.com/install/) or [`podman`](https://podman.io/docs/installation) - Docker or Podman

> :warning: **If you are using Podman please see the notes below**
> 
## JDK target

The project targets language Java level 17.

## Building / Running the Tests

Build the project like this:

```shell
$ mvn clean install
```

The running of the tests can be controlled with the following Maven properties:

| property           | description                                                                               |
|--------------------|-------------------------------------------------------------------------------------------|
| `-DskipUTs=true`   | skip unit tests                                                                           |
| `-DskipITs=true`   | skip integration tests                                                                    |
| `-DskipTests=true` | skip all tests                                                                            |
| `-Pdebug`          | enables logging so you can see what the Kafka clients, Proxy and in VM brokers are up to. |

The kafka environment used by the integrations tests can be _defaulted_ with these two environment variables.

| env var                       | default | description                                                                                                                             |
|-------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `TEST_CLUSTER_EXECUTION_MODE` | `IN_VM` | `IN_VM` or `CONTAINER`. if `IN_VM`, kafka will be run same virtual machines as the integration test. Otherwise containers will be used. |
| `TEST_CLUSTER_KRAFT_MODE`     | `true`  | if true, kafka will be run in kraft mode.                                                                                               |

When the integration-tests are run in `CONTAINER` mode, the kafka/zookeeper logs are written to a location specified by
the `container.logs.dir`  system property. When run through Maven this is defaulted to `integrationtests/target/container-logs`.

Pass the `-Dquick` option to skip all tests and non-essential plug-ins and create the output artifact as quickly as possible:

```shell
$ mvn clean verify -Dquick
```

### Code Formatting

Run the following command to format the source code and organize the imports as per the project's conventions.

```shell
$ mvn process-sources
```
We suggest doing this before opening a Pull Request as the build actions will fail the build for formatting
which doesn't fit our conventions.

## Running Integration Tests on Podman
It maybe necessary to configure the `DOCKER_HOST` environment variable to allow the tests to correctly use test containers.

```bash
DOCKER_HOST=unix://$( podman info --format '{{.Host.RemoteSocket.Path}}')
export DOCKER_HOST
```
There is an incompatibility between HTTP connection timeout expectations of 
[testcontainers-java](https://github.com/testcontainers/testcontainers-java) and the Podman API. This
can result in sporadic test failures when running the Integration Tests under Podman.  It manifests as
failed or hanging REST API calls that lead to test failures and test hangs.

It affects Linux and Mac OS X.
On Linux it manifests as Http calls failing with a `Broken Pipe` exception. 
Similarly on MacOS we see a `localhost:XXX failed to respond`.
To workaround around the issue, turn off the `service_timeout` by following these instructions.

### MacOS X

Start the `podman` machine as normal, then:

```shell
echo 'mkdir -p /etc/containers/containers.conf.d && printf "[engine]\nservice_timeout=0\n" > /etc/containers/containers.conf.d/service-timeout.conf' |  podman machine ssh --username root --
```

### Linux

As a privileged user:

```shell
mkdir -p /etc/containers/containers.conf.d && printf "[engine]\nservice_timeout=0\n" > /etc/containers/containers.conf.d/service-timeout.conf
```

### Verify that the fix is effective

Start this command:
```shell
socat - UNIX-CONNECT:/var/run/docker.sock
```

send this input (including the empty line):
```
GET /version HTTP/1.1
Host: www.example.com

```

You'll see an API response.  If the service_timeout change is effective, the connection
will stay open indefinitely.  If `socat` terminates after about 10seconds, the workaround
has been applied ineffectively.

