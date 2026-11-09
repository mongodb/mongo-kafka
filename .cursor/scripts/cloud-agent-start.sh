#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export JDK17="${JDK17:-$JAVA_HOME}"
export MONGO_KAFKA_TEST_URI="${MONGO_KAFKA_TEST_URI:-mongodb://127.0.0.1:27017/?replicaSet=rs0}"

wait_for_mongo() {
  local attempts=0
  until mongosh --quiet "$MONGO_KAFKA_TEST_URI" --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1; do
    attempts=$((attempts + 1))
    if [[ $attempts -ge 60 ]]; then
      echo "MongoDB did not become ready in time" >&2
      exit 1
    fi
    sleep 1
  done
}

if mongosh --quiet "$MONGO_KAFKA_TEST_URI" --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then
  exit 0
fi

if [[ ! -f /etc/mongod-cloud-dev.conf ]]; then
  echo "mongod-cloud-dev.conf missing; run install first" >&2
  exit 1
fi

sudo -u mongodb mongod --config /etc/mongod-cloud-dev.conf
sleep 2
mongosh --quiet --eval '
  try {
    const s = rs.status();
    if (s.ok !== 1) throw new Error("not initiated");
  } catch (e) {
    rs.initiate({_id:"rs0", members:[{_id:0, host:"127.0.0.1:27017"}]});
  }
' >/dev/null

wait_for_mongo
