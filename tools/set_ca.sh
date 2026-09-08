#!/bin/sh
# Set the Plink coin address on prod without a redeploy:  tools/set_ca.sh 0xYOURCOIN
# Needs ADMIN_SECRET in the environment (railway run tools/set_ca.sh 0x... picks it from Railway Variables).
[ -z "$1" ] && { echo "usage: set_ca.sh 0xADDRESS"; exit 1; }
curl -s -X POST "${SITE_URL:-https://stream-production-3797.up.railway.app}/api/admin" -H "Content-Type: application/json" -H "X-Admin-Secret: $ADMIN_SECRET" -d "{\"action\":\"set\",\"key\":\"ca\",\"value\":\"$1\"}"; echo
