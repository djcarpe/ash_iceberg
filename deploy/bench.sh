#!/bin/bash
# Sequential GraphQL benchmark: each query hits iceberg then delta, cold run + warm run.
run() { curl -s -o /tmp/bench_out.json --max-time 600 -w "%{time_total}" "http://$1.home/gql" -H content-type:application/json -d "$2"; }

declare -A QUERIES=(
  [count_all]='{"query":"{ events(first: 1) { count } }"}'
  [first_page_50]='{"query":"{ events(first: 50) { results { id userId eventType value occurredAt } } }"}'
  [by_user]='{"query":"{ eventsByUser(userId: 4242, first: 50) { results { id eventType value occurredAt } } }"}'
  [by_user_count]='{"query":"{ eventsByUser(userId: 4242, first: 50) { count results { id } } }"}'
  [time_range_1day]='{"query":"{ eventsInRange(from: \"2025-08-01T00:00:00Z\", to: \"2025-08-02T00:00:00Z\", first: 100) { results { id eventType occurredAt } } }"}'
  [time_range_count]='{"query":"{ eventsInRange(from: \"2025-08-01T00:00:00Z\", to: \"2025-08-02T00:00:00Z\", first: 1) { count } }"}'
  [by_type_page]='{"query":"{ eventsByType(eventType: \"purchase\", first: 50) { results { id value occurredAt } } }"}'
  [type_prefix]='{"query":"{ eventsByTypePrefix(prefix: \"log\", first: 50) { results { id eventType } } }"}'
)
ORDER=(count_all first_page_50 by_user by_user_count time_range_1day time_range_count by_type_page type_prefix)

printf "%-18s %10s %10s %10s %10s\n" "query" "ice-cold" "ice-warm" "del-cold" "del-warm"
for q in "${ORDER[@]}"; do
  ic=$(run iceberg "${QUERIES[$q]}"); iw=$(run iceberg "${QUERIES[$q]}")
  dc=$(run delta "${QUERIES[$q]}"); dw=$(run delta "${QUERIES[$q]}")
  printf "%-18s %9ss %9ss %9ss %9ss\n" "$q" "$ic" "$iw" "$dc" "$dw"
done

echo; echo "top_n_by_value (single run each, heavy):"
it=$(run iceberg '{"query":"{ topEvents(limit: 10) { value eventType } }"}')
echo "  iceberg: ${it}s"
dt=$(run delta '{"query":"{ topEvents(limit: 10) { value eventType } }"}')
echo "  delta:   ${dt}s"
