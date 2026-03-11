#!/bin/bash
# MongoDB Kafka Connector - Field Decryption + CS-FLE Demo Runner
#
# This script:
#   1. Builds the MongoDB Kafka Connector JAR (if needed)
#   2. Starts Docker containers (MongoDB, Kafka, Kafka Connect)
#   3. Runs the encryption demo showing both decryption and CS-FLE re-encryption
#
# Requirements:
#   - Docker and Docker Compose
#   - Java 17+ and Gradle (for building the connector)
#   - x86_64 architecture (CS-FLE native libraries require glibc, not compatible with Alpine/ARM64)
#
# Usage:
#   ./run-demo.sh [--rebuild-jar] [--clean]
#
# Options:
#   --rebuild-jar    Force rebuild of the connector JAR
#   --clean          Stop and remove all containers before starting
#
set -e

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[ OK ]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
step()  { echo -e "\n${GREEN}==================================================================${NC}"; echo -e "${GREEN}  $*${NC}"; echo -e "${GREEN}==================================================================${NC}\n"; }

# Parse arguments
REBUILD_JAR=false
CLEAN=false
for arg in "$@"; do
  case $arg in
    --rebuild-jar) REBUILD_JAR=true ;;
    --clean) CLEAN=true ;;
    *) error "Unknown option: $arg"; exit 1 ;;
  esac
done

# Get script directory and project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

step "Step 1: Architecture Check"
ARCH=$(uname -m)
info "Detected architecture: $ARCH"

if [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then
  warn "[WARNING] ARM64 architecture detected!"
  warn "    CS-FLE requires x86_64 due to native library dependencies."
  warn "    This demo will likely fail on ARM64."
  warn ""
  warn "    To run this demo successfully:"
  warn "    1. Use an x86_64 machine, OR"
  warn "    2. Use Docker with x86_64 emulation (slower):"
  warn "       export DOCKER_DEFAULT_PLATFORM=linux/amd64"
  warn ""
  read -p "Continue anyway? (y/N) " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
  fi
fi

step "Step 2: Build MongoDB Kafka Connector JAR"

JAR_PATH="$SCRIPT_DIR/connector-jar/mongo-kafka-connect.jar"

if [ "$REBUILD_JAR" = true ] || [ ! -f "$JAR_PATH" ]; then
  info "Building connector JAR from source..."
  cd "$PROJECT_ROOT"

  if [ ! -f "gradlew" ]; then
    error "gradlew not found in $PROJECT_ROOT"
    error "Please run this script from the demos/field-decryption-and-csfle directory"
    exit 1
  fi

  ./gradlew clean allJar -x test -x spotlessJava

  # Find the built JAR
  BUILT_JAR=$(find build/libs -name "mongo-kafka-connect-*-all.jar" | head -1)

  if [ -z "$BUILT_JAR" ]; then
    error "Failed to build connector JAR"
    exit 1
  fi

  cp "$BUILT_JAR" "$JAR_PATH"
  ok "Connector JAR built and copied to $JAR_PATH"
else
  ok "Using existing connector JAR: $JAR_PATH"
  info "Use --rebuild-jar to force rebuild"
fi

step "Step 3: Start Docker Containers"

cd "$SCRIPT_DIR"

if [ "$CLEAN" = true ]; then
  info "Cleaning up existing containers..."
  docker compose down -v
  ok "Cleanup complete"
fi

# Check if containers are already running
RUNNING_CONTAINERS=$(docker compose ps -q 2>/dev/null | wc -l)

if [ "$RUNNING_CONTAINERS" -gt 0 ]; then
  info "Containers are already running. Checking health..."

  # Check if Kafka Connect is accessible
  if curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
    ok "Containers are healthy and running"
  else
    warn "Containers are running but not healthy. Restarting..."
    docker compose down
    info "Starting MongoDB, Kafka, and Kafka Connect..."
    docker compose up --build -d
    ok "Containers restarted"
  fi
else
  info "Starting MongoDB, Kafka, and Kafka Connect..."

  # Try to start containers, handle port conflicts
  if ! docker compose up --build -d 2>&1; then
    warn "Failed to start containers (possibly due to port conflicts)"
    info "Attempting to stop conflicting containers..."

    # Stop any containers using port 8083
    CONFLICTING=$(docker ps --filter "publish=8083" -q)
    if [ -n "$CONFLICTING" ]; then
      warn "Stopping containers using port 8083..."
      docker stop $CONFLICTING
    fi

    # Try again
    info "Retrying container startup..."
    docker compose up --build -d
  fi

  ok "Containers started"
fi

step "Step 4: Wait for Services to be Ready"

info "Waiting for Kafka Connect to be ready (this may take 30-60 seconds)..."
for i in $(seq 1 60); do
  if curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
    ok "Kafka Connect is ready!"
    break
  fi

  if [ "$i" -eq 60 ]; then
    error "Kafka Connect did not become ready in time"
    error "Check logs with: docker logs kafka-connect"
    exit 1
  fi

  sleep 2
done

step "Step 5: Run Encryption Demo"

info "Starting the field decryption + CS-FLE demo..."
echo ""

bash "$SCRIPT_DIR/demo-encryption-with-csfle.sh"

DEMO_RESULT=$?

if [ $DEMO_RESULT -eq 0 ]; then
  echo ""
  step "  Demo Completed Successfully!"
  echo ""
  info "What happened:"
  echo "  1. Sample employee data was encrypted with AES (simulating legacy system encryption)"
  echo "  2. Data was published to Kafka (still encrypted)"
  echo "  3. Sink Connector decrypted the data using SampleAesFieldValueTransformer"
  echo "  4. Sink Connector re-encrypted the data using MongoDB CS-FLE"
  echo "  5. Data is now stored in MongoDB, encrypted with MongoDB's encryption"
  echo ""
  info "The data is protected at every stage of the pipeline!"
  echo ""
else
  echo ""
  step "  Demo Failed"
  echo ""
  error "The demo encountered an error. Check the logs above for details."
  error "You can also check container logs:"
  echo "    docker logs kafka-connect"
  echo "    docker logs mongo1"
  echo ""
  exit 1
fi

step "Cleanup"
echo ""
info "To stop and remove all containers:"
echo "    cd $SCRIPT_DIR && docker compose down -v"
echo ""
info "To view connector status:"
echo "    curl http://localhost:8083/connectors/csfle-sink/status | jq ."
echo ""
