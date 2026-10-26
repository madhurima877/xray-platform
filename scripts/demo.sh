#!/bin/sh
# End-to-end check against a running `docker-compose up` stack:
# admin login → keyword + level edit → messages through xray-api → stats,
# history, White List candidates, KISA reports and sync status.
#
# Usage: XRAY=http://localhost:8080 ADMIN=http://localhost:8090 ./scripts/demo.sh
set -eu

XRAY="${XRAY:-http://localhost:8080}"
ADMIN="${ADMIN:-http://localhost:8090}"
USER="${ADMIN_USER:-admin}"
PASS="${ADMIN_PASSWORD:-change-me-now}"

j() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

echo "== login"
TOKEN=$(curl -sf "$ADMIN/api/login" -d "{\"username\":\"$USER\",\"password\":\"$PASS\"}" | j 'd["token"]')
AUTH="Authorization: Bearer $TOKEN"
api() { curl -sf -H "$AUTH" -H 'Content-Type: application/json' "$@"; }

echo "== customers copied by master-sync"
api "$ADMIN/api/customers" | j '[(c["id"], c["level"], c["api_keys"]) for c in d["items"]]'

echo "== set cust-demo to T5, keyword hits BLOCK"
api -X PATCH "$ADMIN/api/customers/cust-demo" -d '{"level":5,"keyword_action":"BLOCK"}' | j 'd["redis_refreshed"]'

echo "== add keyword"
api "$ADMIN/api/keywords" -d '{"phrase":"Lottery   Winner"}' 2>/dev/null | j 'd["keyword"]["phrase"]' 2>/dev/null || echo "(already there)"

echo "== Black List has the KISA feed"
api "$ADMIN/api/lists/black?source=kisa" | j '[e["value"] for e in d["items"]]'

echo "== send messages through xray-api"
send() {
  curl -s "$XRAY/inspect" -H "X-API-Key: $1" -d "{\"message_id\":\"$2\",\"sender\":\"$3\",\"text\":\"$4\"}"; echo
}
RUN=$(date +%s)
sleep 31 # xray-api reloads keywords every 30 s (customer copy every 10 s)
send demo-key "d-$RUN-1" "+911111" "You are a lottery winner, claim now"
send demo-key "d-$RUN-2" "+912222" "Login at http://evil.example/login"
for i in 1 2 3 4; do
  send shop-key "s-$RUN-$i" "+919999" "Your order ships today https://shop-safe.example/track/$i"
done
sleep 6

echo "== dashboard (stats-worker)"
api "$ADMIN/api/stats/summary" | j 'd["by_verdict"]'

echo "== history (audit-worker)"
api "$ADMIN/api/history?limit=10" | j '[(h["message_id"], h["verdict"]) for h in d["items"]]'

echo "== White List candidates (wl-collector)"
api "$ADMIN/api/wl-candidates" | j '[(c["customer_id"], c["kind"], c["value"], c["allow_count"]) for c in d["items"]]'

echo "== approve the shop host"
api "$ADMIN/api/wl-candidates/approve" -d '{"customer_id":"cust-shop","kind":"url_host","value":"shop-safe.example"}' \
  | j 'd["candidate"]["status"]'

echo "== KISA reports (kisa-reporter)"
api "$ADMIN/api/kisa/reports" | j '(d["counts"], [r["message_id"] for r in d["items"]])'

echo "== sync jobs"
api -X POST "$ADMIN/api/sync/keyword-sync" | j 'd["status"]'
sleep 2
api "$ADMIN/api/sync/status" | j '[(s["job"], s["last_run"] and s["last_run"]["ok"]) for s in d["jobs"]]'

echo "== audit log"
api "$ADMIN/api/audit?limit=5" | j '[(a["username"], a["action"], a["entity"]) for a in d["items"]]'
