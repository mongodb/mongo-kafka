#!/usr/bin/env bash

set -o errexit  # Exit the script with error if any of the commands fail

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "Usage: $0 <current version> <release version> <next release version> <remote (defaults to origin)>" >&2
  exit 1
fi

CURRENT=$1
RELEASE=$2
NEXT=$3
REMOTE=${4:-origin}
BRANCH=`git rev-parse --abbrev-ref HEAD`

echo "Current version: ${CURRENT}"
echo "Release: ${RELEASE}"
echo "Next version: ${NEXT}"
echo "Remote: ${REMOTE}"
echo "Branch: ${BRANCH}"

[[ "$(read -e -p 'Continue? [y/N]> '; echo $REPLY)" == [Yy]* ]]

# Update to release version
echo "Updating build.gradle.kts to release version ${RELEASE}"
sed -i '' "s/version = \"${CURRENT}\"/version = \"${RELEASE}\"/g" build.gradle.kts
git add build.gradle.kts
git commit -m "Version: bump ${RELEASE}"
git push ${REMOTE} ${BRANCH}

echo "Sleeping for 5 ..."
sleep 5s

# Tag it
echo "Tagging release ${RELEASE}"
git tag -a r${RELEASE} -m "${RELEASE}"
git push ${REMOTE} r${RELEASE}

echo "Sleeping for 5 ..."
sleep 5s

# Update to next snapshot version
echo "Updating build.gradle.kts to next snapshot version ${NEXT}"
sed -i '' "s/version = \"${RELEASE}\"/version = \"${NEXT}\"/g" build.gradle.kts
git add build.gradle.kts
git commit -m "Version: bump ${NEXT}"
git push ${REMOTE} ${BRANCH}
