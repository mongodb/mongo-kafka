#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export JDK17="${JDK17:-$JAVA_HOME}"

install_java() {
  if [[ -x "$JAVA_HOME/bin/java" ]] && "$JAVA_HOME/bin/java" -version 2>&1 | grep -q 'version "17'; then
    :
  else
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-17-jdk
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export JDK17="$JAVA_HOME"
  fi
  sudo tee /etc/profile.d/mongo-kafka-java.sh >/dev/null <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export JDK17="$JAVA_HOME"
export PATH="$JAVA_HOME/bin:$PATH"
export MONGO_KAFKA_TEST_URI=mongodb://127.0.0.1:27017/?replicaSet=rs0
EOF
}

install_mongodb() {
  if command -v mongod >/dev/null && command -v mongosh >/dev/null; then
    return 0
  fi
  if [[ ! -f /usr/share/keyrings/mongodb-server-7.0.gpg ]]; then
    curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc |
      sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
    echo 'deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse' |
      sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list >/dev/null
    sudo apt-get update -qq
  fi
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mongodb-org
}

ensure_mongodb_dirs() {
  sudo mkdir -p /var/lib/mongodb-rs /var/log/mongodb-rs /var/run/mongodb
  sudo chown -R mongodb:mongodb /var/lib/mongodb-rs /var/log/mongodb-rs /var/run/mongodb 2>/dev/null || true
  if [[ ! -f /etc/mongod-cloud-dev.conf ]]; then
    sudo tee /etc/mongod-cloud-dev.conf >/dev/null <<'EOF'
storage:
  dbPath: /var/lib/mongodb-rs
systemLog:
  destination: file
  path: /var/log/mongodb-rs/mongod.log
  logAppend: true
net:
  port: 27017
  bindIp: 127.0.0.1
processManagement:
  fork: true
  pidFilePath: /var/run/mongodb/mongod.pid
replication:
  replSetName: rs0
EOF
  fi
}

install_java
install_mongodb
ensure_mongodb_dirs

cd "$(dirname "$0")/../.."
./gradlew compileJava test
