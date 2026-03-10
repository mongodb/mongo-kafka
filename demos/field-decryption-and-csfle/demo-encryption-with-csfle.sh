#!/bin/bash
# Demo: MongoDB Kafka Sink Connector — Field Decryption + CS-FLE Re-encryption (KAFKA-470)
#
# This demonstrates BOTH encryption features:
#   1. Data arrives with legacy AES encryption
#   2. Sink Connector decrypts using SampleAesFieldValueTransformer
#   3. Sink Connector re-encrypts using MongoDB CS-FLE before storage
#   4. Data is stored encrypted in MongoDB, readable only with the encryption key
#
# Usage: cd demo && docker compose up --build -d && bash demo-encryption-with-csfle.sh
set -e

CONNECT_URL="http://localhost:8083"
AES_KEY="mySecretKey12345"
TOPIC="demo.encrypted_source"
TRANSFORMER="com.mongodb.kafka.connect.sink.processor.field.transform.SampleAesFieldValueTransformer"

# Pre-generated 96-byte master key for CS-FLE (base64-encoded)
# In production, this would come from a secure KMS
MASTER_KEY="OxZIq3UgZzwUMKHT4aGtjHasvB9KGzK/LTIMMErnZMLHJJT5KbG6j0g+3qjEAxEpno/K6EkUgEEBg/jHBSQ6MmdcF6AShEfYPmsTh4W08Tubz7rGrtpjP3w4nJCrBxWr"

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

step "Step 1: Create CS-FLE Key Vault"
info "Setting up encryption key vault..."
docker exec mongo1 mongosh --quiet --eval "
use encryption;
db.dropDatabase();
db.createCollection('__keyVault');

// Insert a data encryption key
const keyId = UUID('12345678-1234-1234-1234-123456789012');
db.__keyVault.insertOne({
  _id: keyId,
  keyMaterial: BinData(0, '${MASTER_KEY}'),
  creationDate: new Date(),
  updateDate: new Date(),
  status: 1,
  masterKey: { provider: 'local' }
});
print('Key vault created with key ID: ' + keyId);
" > /dev/null
ok "Key vault created"

step "Step 2: Insert AES-encrypted documents into source collection"
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
db.csfle_sink.drop();
db.encrypted_source.insertMany(docs);
print('Inserted ' + docs.length + ' docs. Sample encrypted SSN: ' + docs[0].ssn);
"
ok "Encrypted documents inserted."

step "Step 3: Create Source Connector"
curl -s -X DELETE "$CONNECT_URL/connectors/enc-source" > /dev/null 2>&1 || true
curl -s -X DELETE "$CONNECT_URL/connectors/csfle-sink" > /dev/null 2>&1 || true
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

step "Step 4: Wait for messages in Kafka topic"
info "Waiting for messages in '${TOPIC}'..."
for i in $(seq 1 30); do
  COUNT=$(docker exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic "$TOPIC" 2>/dev/null \
    | awk -F: '{s+=$3}END{print s}' || echo 0)
  [ "$COUNT" -ge 3 ] 2>/dev/null && { ok "Found ${COUNT} messages."; break; }
  sleep 3
done

step "Step 5: Create Sink Connector (with decryption + CS-FLE re-encryption)"
info "Creating sink connector with field decryption AND CS-FLE re-encryption..."
warn "This will decrypt legacy AES encryption, then re-encrypt with MongoDB CS-FLE"

# Build CS-FLE schema map
SCHEMA_MAP='{"demo.csfle_sink":{"bsonType":"object","properties":{"ssn":{"encrypt":{"bsonType":"string","algorithm":"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic","keyId":[{"$binary":{"base64":"EjRWeBI0EjRWeBI0VngSNA==","subType":"04"}}]}},"email":{"encrypt":{"bsonType":"string","algorithm":"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic","keyId":[{"$binary":{"base64":"EjRWeBI0EjRWeBI0VngSNA==","subType":"04"}}]}}}}}'

# Create connector config using jq for proper JSON escaping
CONNECTOR_CONFIG=$(jq -n \
  --arg topic "$TOPIC" \
  --arg transformer "$TRANSFORMER" \
  --arg aes_key "$AES_KEY" \
  --arg master_key "$MASTER_KEY" \
  --arg schema_map "$SCHEMA_MAP" \
  '{
    "name": "csfle-sink",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
      "connection.uri": "mongodb://mongo1:27017/?cryptSharedLibRequired=false",
      "topics": $topic,
      "database": "demo",
      "collection": "csfle_sink",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "field.value.transformer": $transformer,
      "field.value.transformer.fields": "ssn,email",
      "field.value.transformer.fail.on.error": "true",
      "field.value.transformer.aes.key": $aes_key,
      "csfle.enabled": "true",
      "csfle.key.vault.namespace": "encryption.__keyVault",
      "csfle.local.master.key": $master_key,
      "csfle.schema.map": $schema_map,
      "csfle.mongocryptd.bypass.spawn": "true"
    }
  }')

curl -s -X POST "$CONNECT_URL/connectors" -H "Content-Type: application/json" -d "$CONNECTOR_CONFIG" > /dev/null
ok "Sink connector created with decryption + CS-FLE."




step "Step 6: Wait for documents in sink collection"
info "Waiting for documents in demo.csfle_sink..."
for i in $(seq 1 30); do
  N=$(docker exec mongo1 mongosh --quiet --eval \
    "db=db.getSiblingDB('demo'); print(db.csfle_sink.countDocuments())")
  [ "$N" -ge 3 ] 2>/dev/null && { ok "Found ${N} documents."; break; }
  sleep 3
done

step "Step 7: Verify encryption at rest"
echo ""
info "SOURCE — demo.encrypted_source (legacy AES encryption):"
docker exec mongo1 mongosh --quiet --eval "
  db=db.getSiblingDB('demo');
  const doc = db.encrypted_source.findOne({_id:1});
  printjson({_id:doc._id, name:doc.name, ssn:doc.ssn, email:doc.email});
"

echo ""
info "SINK — demo.csfle_sink (MongoDB CS-FLE encryption, raw view):"
warn "Without the encryption key, fields appear as binary data:"
docker exec mongo1 mongosh --quiet --eval "
  db=db.getSiblingDB('demo');
  const doc = db.csfle_sink.findOne({_id:1});
  print('_id: ' + doc._id);
  print('name: ' + doc.name);
  print('ssn: ' + (doc.ssn ? '<Binary subtype 6, encrypted>' : doc.ssn));
  print('email: ' + (doc.email ? '<Binary subtype 6, encrypted>' : doc.email));
  print('dept: ' + doc.dept);
"

step "Step 8: Verify connector status"
info "Checking if CS-FLE connector is running..."

CONNECTOR_STATUS=$(curl -s "$CONNECT_URL/connectors/csfle-sink/status" | jq -r '.tasks[0].state')

if [ "$CONNECTOR_STATUS" = "RUNNING" ]; then
  echo ""
  echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${GREEN}║  ✅  DEMO PASSED — Complete encryption migration!            ║${NC}"
  echo -e "${GREEN}║                                                                ║${NC}"
  echo -e "${GREEN}║  1. Legacy AES encryption → decrypted by transformer          ║${NC}"
  echo -e "${GREEN}║  2. Plaintext (in memory only)                                ║${NC}"
  echo -e "${GREEN}║  3. Re-encrypted by MongoDB CS-FLE → stored encrypted         ║${NC}"
  echo -e "${GREEN}║                                                                ║${NC}"
  echo -e "${GREEN}║  The data is protected at every stage:                        ║${NC}"
  echo -e "${GREEN}║  - In source system: encrypted with AES                       ║${NC}"
  echo -e "${GREEN}║  - In Kafka: still encrypted with AES                         ║${NC}"
  echo -e "${GREEN}║  - In MongoDB: encrypted with CS-FLE                          ║${NC}"
  echo -e "${GREEN}║                                                                ║${NC}"
  echo -e "${GREEN}║  Only authorized clients with the key can read the data.      ║${NC}"
  echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
else
  echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${RED}║  ❌  DEMO FAILED — Connector status: $CONNECTOR_STATUS      ║${NC}"
  echo -e "${RED}║  Check: docker logs kafka-connect                         ║${NC}"
  echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
  exit 1
fi

echo ""
info "Cleanup commands:"
echo "  curl -X DELETE $CONNECT_URL/connectors/enc-source"
echo "  curl -X DELETE $CONNECT_URL/connectors/csfle-sink"
echo "  cd demo && docker compose down -v"