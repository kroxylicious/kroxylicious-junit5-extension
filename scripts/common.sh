#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -Eu

OS=$(uname)

resolveCommand () {
  local targetCommand=${1}
  local resolvedCommand
  resolvedCommand=$(command -v "${targetCommand}")
  if [[ -z ${resolvedCommand} ]]; then
    >&2 echo -e "\033[0;31m Unable to resolve path to ${targetCommand}\033[0m"
    exit 127
  else
    echo "${resolvedCommand}"
  fi
}

if [ "$OS" = 'Darwin' ]; then
  # for MacOS
  SED=$(resolveCommand gsed)
else
  # for Linux and Windows
  SED=$(resolveCommand sed)
fi
export SED

