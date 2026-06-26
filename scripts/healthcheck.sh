#!/usr/bin/env bash
# =============================================================================
# healthcheck.sh — quick "is the pipeline working?" check.
# -----------------------------------------------------------------------------
# Runs a series of read-only checks against the running Docker stack and prints a
# pass/fail summary. Safe to run any time; it changes nothing.
#
# Usage:
#   bash scripts/healthcheck.sh
#   make check                       # same thing via the Makefile
# =============================================================================
set -uo pipefail

KAFKA=rtep-kafka
SPARK=rtep-spark-streaming
PRODUCER=rtep-producer
EVENTS_TOPIC=events
DLQ_TOPIC=dead-letter-events

# ---- pretty helpers ---------------------------------------------------------
pass() { printf '  \033[32m✓ PASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31m✗ FAIL\033[0m  %s\n' "$1"; }
warn() { printf '  \033[33m! WARN\033[0m  %s\n' "$1"; }
hdr()  { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

FAILS=0

# ---- 0. Docker available ----------------------------------------------------
hdr "0. Docker daemon"
if docker info >/dev/null 2>&1; then
  pass "Docker is running"
else
  fail "Docker daemon is not running — start Docker Desktop"
  exit 1
fi

# ---- 1. Container status ----------------------------------------------------
hdr "1. Containers"
docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null
for c in "$KAFKA" "$SPARK" "$PRODUCER"; do
  state="$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)"
  if [ "$state" = "running" ]; then pass "$c is running"; else fail "$c is '$state'"; FAILS=$((FAILS+1)); fi
done

# ---- 2. Kafka topic has events ---------------------------------------------
hdr "2. Events flowing into Kafka"
offsets="$(docker exec "$KAFKA" bash -c \
  "kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $EVENTS_TOPIC --time -1" 2>/dev/null)"
if [ -n "$offsets" ]; then
  total="$(echo "$offsets" | awk -F: '{s+=$3} END{print s+0}')"
  echo "$offsets" | sed 's/^/    /'
  if [ "$total" -gt 0 ]; then pass "events topic has $total message(s)"; else warn "events topic is empty (producer may be warming up)"; fi
else
  fail "could not read offsets for '$EVENTS_TOPIC' (topic missing?)"; FAILS=$((FAILS+1))
fi

# ---- 3. Sample one event (is the data shape correct?) ----------------------
hdr "3. Sample event (check the data is correct)"
sample="$(docker exec "$KAFKA" bash -c \
  "kafka-console-consumer --bootstrap-server localhost:9092 --topic $EVENTS_TOPIC --from-beginning --max-messages 1 --timeout-ms 8000" 2>/dev/null | head -1)"
if [ -n "$sample" ]; then
  echo "    $sample"
  if echo "$sample" | grep -q '"symbol"' && echo "$sample" | grep -q '"price"'; then
    pass "event JSON has expected fields (symbol, price, ...)"
  else
    warn "event present but missing expected fields — inspect above"
  fi
else
  warn "no sample event read (topic may be empty)"
fi

# ---- 4. Spark is processing / writing --------------------------------------
hdr "4. Spark streaming output"
logs="$(docker compose logs "$SPARK" 2>/dev/null)"
wrote="$(echo "$logs" | grep -cE '\] wrote [0-9]+ row')"
errs="$(echo "$logs" | grep -cE 'Fatal error|AnalysisException|Access Denied|BigQueryException')"
started="$(echo "$logs" | grep -c 'All 4 streaming queries started')"
[ "$started" -gt 0 ] && pass "streaming queries started" || warn "no 'queries started' line yet (still booting?)"
if [ "$wrote" -gt 0 ]; then pass "$wrote sink-write line(s) found ([raw_events] wrote N rows ...)"; else warn "no sink writes yet (console mode prints batches instead; or still warming up)"; fi
if [ "$errs" -gt 0 ]; then fail "$errs error line(s) in spark logs — see: docker compose logs $SPARK | grep -E 'Fatal|Exception|Denied'"; FAILS=$((FAILS+1)); else pass "no Fatal/Exception/Denied errors in spark logs"; fi

# ---- 5. Crash-loop check ----------------------------------------------------
hdr "5. Stability"
rc="$(docker inspect -f '{{.RestartCount}}' "$SPARK" 2>/dev/null || echo '?')"
if [ "$rc" = "0" ]; then pass "spark-streaming RestartCount=0 (stable)"
elif [ "$rc" = "?" ]; then warn "could not read restart count"
else warn "spark-streaming RestartCount=$rc (a climbing number = crash loop → check logs)"; fi

# ---- summary ----------------------------------------------------------------
hdr "Summary"
if [ "$FAILS" -eq 0 ]; then
  printf '  \033[32mAll critical checks passed.\033[0m\n'
  printf '  Tip: verify data landed in BigQuery with:  make verify-bq\n'
  exit 0
else
  printf '  \033[31m%d critical check(s) failed — see above.\033[0m\n' "$FAILS"
  exit 1
fi
