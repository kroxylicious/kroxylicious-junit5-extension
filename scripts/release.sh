#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -e

REPOSITORY="origin"
BRANCH_FROM="main"
SNAPSHOT_INCREMENT_INDEX=2
while getopts ":v:b:r:k:n:" opt; do
  case $opt in
    v) RELEASE_VERSION="${OPTARG}"
    ;;
    b) BRANCH_FROM="${OPTARG}"
    ;;
    r) REPOSITORY="${OPTARG}"
    ;;
    k) GPG_KEY="${OPTARG}"
    ;;
    n) SNAPSHOT_INCREMENT_INDEX="${OPTARG}"
    ;;

    \?) echo "Invalid option -${OPTARG}" >&2
    exit 1
    ;;
  esac

  case ${OPTARG} in
    -*) echo "Option $opt needs a valid argument"
    exit 1
    ;;
  esac
done

if [[ -z "${GPG_KEY}" ]]; then
    echo "GPG_KEY not set unable to sign the release. Please specify -k <YOUR_GPG_KEY>" 1>&2
    exit 1
fi

if [[ -z ${RELEASE_VERSION} ]]; then
  echo "No version specified aborting"
  exit 1
fi

GREEN='\033[0;32m'
NC='\033[0m' # No Color

git stash --all
echo "Creating release branch from ${BRANCH_FROM}"
git fetch -q "${REPOSITORY}"
RELEASE_DATE=$(date -u '+%Y-%m-%d')
git checkout -b "prepare-release-${RELEASE_VERSION}-${RELEASE_DATE}" "${REPOSITORY}/${BRANCH_FROM}"

#Disable the shell check as the colour codes only work with interpolation.
# shellcheck disable=SC2059
printf "Validating the build is ${GREEN}green${NC}\n"
mvn -q clean verify

echo "Versioning Kroxylicious-junit-extension as ${RELEASE_VERSION}"

mvn -q versions:set -DnewVersion="${RELEASE_VERSION}" -DgenerateBackupPoms=false
echo "Validating things still build"
mvn -q clean install -Pquick

echo "Committing framework release to git"
git add '**/pom.xml' 'pom.xml'
git commit --message "Release Framework version v${RELEASE_VERSION}" --signoff

git tag -f "v${RELEASE_VERSION}"

git push --tags

echo "Deploying release to maven central"
mvn deploy -Prelease -DskipTests=true -DreleaseSigningKey="${GPG_KEY}"

git checkout -b "prepare-development-${RELEASE_DATE}" "prepare-release-${RELEASE_VERSION}-${RELEASE_DATE}"
mvn versions:set -DnextSnapshot=true -DnextSnapshotIndexToIncrement="${SNAPSHOT_INCREMENT_INDEX}" -DgenerateBackupPoms=false

git add '**/pom.xml' 'pom.xml'
git commit --message "Start next development version" --signoff

if ! command -v gh &> /dev/null
then
    echo "gh command could not be found. Please create a pull request by hand https://github.com/kroxylicious/kroxylicious/compare"
    exit
fi

echo "Create pull request to merge the released version & update to new snapshot version for development"
gh pr create --base main --title "Kroxylicious junit extension development version ${RELEASE_DATE}" --body "prepare for new development version"
