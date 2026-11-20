#!/usr/bin/env bash
# Tạo N ví W0..W(N-1), mỗi ví BALANCE tiền. Dùng: ./scripts/seed-wallets.sh [N=1000] [BALANCE=1000000]
N=${1:-1000}; BAL=${2:-1000000}; URL=${WALLET_URL:-http://localhost:8081}
seq 0 $((N-1)) | xargs -P 20 -I{} curl -s -o /dev/null -XPOST "$URL/wallets" \
  -H 'Content-Type: application/json' -d "{\"id\":\"W{}\",\"balance\":$BAL}"
echo "seeded $N wallets"
