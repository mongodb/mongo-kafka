#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export JDK17="${JDK17:-$JAVA_HOME}"
export MONGO_KAFKA_TEST_URI="${MONGO_KAFKA_TEST_URI:-mongodb://127.0.0.1:27017/?replicaSet=rs0}"
DIRECT_URI="mongodb://127.0.0.1:27017"

echo "mongo-kafka start: checking MongoDB replica set rs0"

mongod_listening() {
  mongosh --quiet "$DIRECT_URI" --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1
}

replica_set_ready() {
  mongosh --quiet "$MONGO_KAFKA_TEST_URI" --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1
}

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

if replica_set_ready; then
  echo "mongo-kafka start: MongoDB replica set already ready"
  exit 0
fi

if [[ ! -f /etc/mongod-cloud-dev.conf ]]; then
  echo "mongo-kafka start: mongod-cloud-dev.conf missing; run install first" >&2
  exit 1
fi

if ! mongod_listening; then
  sudo rm -f /var/run/mongodb/mongod.pid
  echo "mongo-kafka start: launching mongod (replSet rs0)"
  sudo -u mongodb mongod --config /etc/mongod-cloud-dev.conf
  sleep 2
fi
mongosh --quiet --eval '
  try {
    const s = rs.status();
    if (s.ok !== 1) throw new Error("not initiated");
  } catch (e) {
    rs.initiate({_id:"rs0", members:[{_id:0, host:"127.0.0.1:27017"}]});
  }
' >/dev/null

wait_for_mongo
echo "mongo-kafka start: MongoDB replica set ready at $MONGO_KAFKA_TEST_URI"
