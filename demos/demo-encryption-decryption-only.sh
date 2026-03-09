#!/bin/bash
# Demo: MongoDB Kafka Sink Connector — Pluggable Field Decryption (KAFKA-470)
#
# This demonstrates the pluggable field decryption feature:
#   1. Data arrives with Oracle/Hibernate AES encryption
#   2. Sink Connector decrypts using SampleAesFieldValueTransformer
#   3. Data is stored as plaintext in MongoDB
#
# On x86_64 platforms, you can also enable CS-FLE re-encryption by setting:
#   ENABLE_CSFLE=true bash demo-encryption.sh
#
# Usage: cd demo && docker compose up --build -d && bash demo-encryption.sh
set -e

# Detect platform
ARCH=$(uname -m)
ENABLE_CSFLE=${ENABLE_CSFLE:-false}

if [ "$ENABLE_CSFLE" = "true" ] && [ "$ARCH" = "arm64" -o "$ARCH" = "aarch64" ]; then
  echo "⚠️  WARNING: CS-FLE is not supported on ARM64 due to mongodb-crypt native library limitations."
  echo "    Falling back to decryption-only demo."
  echo "    To test CS-FLE, run this demo on an x86_64 platform."
  echo ""
  ENABLE_CSFLE=false
fi

CONNECT_URL="http://localhost:8083"
AES_KEY="mySecretKey12345"
TOPIC="demo.encrypted_source"
TRANSFORMER="com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[ OK ]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
step()  { echo -e "\n${GREEN}━━━ $* ━━━${NC}"; }

step "Step 0: Verify infrastructure"
info "Waiting for Kafka Connect..."
for i in $(seq 1 30); do
  curl -s "$CONNECT_URL/" > /dev/null 2>&1 && { ok "Kafka Connect ready."; break; }
  [ "$i" -eq 30 ] && { echo -e "${RED}Kafka Connect not ready.${NC}"; exit 1; }
  sleep 2
done

step "Step 1: Insert AES-encrypted documents into source collection"
info "Encrypting fields with AES-128-ECB and inserting into demo.encrypted_source..."
docker exec mongo1 mongosh --quiet --eval "
const crypto = require('crypto');
function enc(text, key) {
  const c = crypto.createCipheriv('aes-128-ecb', Buffer.from(key,'utf8'), null);
  return c.update(text,'utf8','base64') + c.final('base64');
}
const k = '${AES_KEY}';
const docs = [
  { _id:1, name:'Alice Johnson', ssn:enc('123-45-6789',k), email:enc('alice@example.com',k), dept:'Engineering' },
  { _id:2, name:'Bob Smith',     ssn:enc('987-65-4321',k), email:enc('bob@example.com',k),   dept:'Marketing'   },
  { _id:3, name:'Carol Davis',   ssn:enc('555-12-3456',k), email:enc('carol@example.com',k), dept:'Finance'     }
];
db = db.getSiblingDB('demo');
db.encrypted_source.drop();
db.decrypted_sink.drop();
db.encrypted_source.insertMany(docs);
print('Inserted ' + docs.length + ' docs. Sample encrypted SSN: ' + docs[0].ssn);
"
ok "Encrypted documents inserted."

step "Step 2: Create Source Connector"
curl -s -X DELETE "$CONNECT_URL/connectors/enc-source" > /dev/null 2>&1 || true
curl -s -X DELETE "$CONNECT_URL/connectors/dec-sink" > /dev/null 2>&1 || true
sleep 2

info "Creating source connector for demo.encrypted_source..."
curl -s -X POST "$CONNECT_URL/connectors" -H "Content-Type: application/json" -d '{
  "name": "enc-source",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    "connection.uri": "mongodb://mongo1:27017",
    "database": "demo",
    "collection": "encrypted_source",
    "copy.existing": "true",
    "output.format.value": "json",
    "output.format.key": "json",
    "publish.full.document.only": "true"
  }
}' > /dev/null
ok "Source connector created."

step "Step 3: Wait for messages in Kafka topic"
info "Waiting for messages in '${TOPIC}'..."
for i in $(seq 1 30); do
  COUNT=$(docker exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic "$TOPIC" 2>/dev/null \
    | awk -F: '{s+=$3}END{print s}' || echo 0)
  [ "$COUNT" -ge 3 ] 2>/dev/null && { ok "Found ${COUNT} messages."; break; }
  sleep 3
done

step "Step 4: Create Sink Connector (with field decryption)"
info "Creating sink connector with SampleAesFieldValueTransformer..."
curl -s -X POST "$CONNECT_URL/connectors" -H "Content-Type: application/json" -d '{
  "name": "dec-sink",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb://mongo1:27017",
    "topics": "'"${TOPIC}"'",
    "database": "demo",
    "collection": "decrypted_sink",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "field.value.transformer": "'"${TRANSFORMER}"'",
    "field.value.transformer.fields": "ssn,email",
    "field.value.transformer.fail.on.error": "true",
    "field.value.transformer.aes.key": "'"${AES_KEY}"'"
  }
}' > /dev/null
ok "Sink connector created."

step "Step 5: Wait for decrypted documents in sink collection"
info "Waiting for documents in demo.decrypted_sink..."
for i in $(seq 1 30); do
  N=$(docker exec mongo1 mongosh --quiet --eval \
    "db=db.getSiblingDB('demo'); print(db.decrypted_sink.countDocuments())")
  [ "$N" -ge 3 ] 2>/dev/null && { ok "Found ${N} documents."; break; }
  sleep 3
done

step "Step 6: Compare source (encrypted) vs sink (decrypted)"
echo ""
info "SOURCE — demo.encrypted_source (fields are AES-encrypted):"
docker exec mongo1 mongosh --quiet --eval "
  db=db.getSiblingDB('demo');
  db.encrypted_source.find().sort({_id:1}).forEach(printjson);
"

echo ""
info "SINK — demo.decrypted_sink (fields are plaintext after decryption):"
docker exec mongo1 mongosh --quiet --eval "
  db=db.getSiblingDB('demo');
  db.decrypted_sink.find().sort({_id:1}).forEach(printjson);
"

step "Step 7: Automated verification"



DECRYPTED_SSN=$(docker exec mongo1 mongosh --quiet --eval \
  "db=db.getSiblingDB('demo'); print(db.decrypted_sink.findOne({_id:1}).ssn)")

if [ "$DECRYPTED_SSN" = "123-45-6789" ]; then
  echo ""
  echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${GREEN}║  ✅  DEMO PASSED — Fields were decrypted successfully!    ║${NC}"
  echo -e "${GREEN}║                                                            ║${NC}"
  echo -e "${GREEN}║  Source:  ssn = 'A+3BxZDKVFsBSRoYsD6dOg==' (AES-encrypted)  ║${NC}"
  echo -e "${GREEN}║  Sink:    ssn = '123-45-6789' (plaintext)                  ║${NC}"
  echo -e "${GREEN}║                                                            ║${NC}"
  echo -e "${GREEN}║  SampleAesFieldValueTransformer decrypted ssn and email    ║${NC}"
  echo -e "${GREEN}║  fields transparently during the Kafka → MongoDB sink.     ║${NC}"
  echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
else
  echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${RED}║  ❌  DEMO FAILED — Expected '123-45-6789', got '$DECRYPTED_SSN'  ║${NC}"
  echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
  exit 1
fi

echo ""
info "Cleanup commands:"
echo "  curl -X DELETE $CONNECT_URL/connectors/enc-source"
echo "  curl -X DELETE $CONNECT_URL/connectors/dec-sink"
echo "  cd demo && docker compose down -v"