#!/bin/bash
set -e

# Generate config.yaml from environment variables, then start the backend.
mkdir -p /data

cat > /data/config.yaml << EOF
telegram:
  api_id: ${TGFS_API_ID:-0}
  api_hash: ${TGFS_API_HASH:-dev_placeholder}
  bot:
    session_file: bot.session
    tokens:
      - ${TGFS_BOT_TOKEN:-dev_placeholder}
  private_file_channel:
    - '${TGFS_CHANNEL_ID:-0}'
  lib: pyrogram
tgfs:
  users:
    ${TGFS_USERNAME:-admin}:
      password: ${TGFS_PASSWORD:-admin}
      readonly: false
  jwt:
    secret: ${TGFS_JWT_SECRET:-dev_jwt_secret_placeholder}
    algorithm: HS256
    life: 604800
  metadata:
    '${TGFS_CHANNEL_ID:-0}':
      name: default
      type: pinned_message
  server:
    host: 0.0.0.0
    port: 1900
  sftp:
    enabled: false
EOF

echo "Generated config.yaml at /data/config.yaml"
exec python main.py
