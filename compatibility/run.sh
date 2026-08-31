#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

./sbt stage
temporary_root="${TMPDIR:-/tmp}"
broker_data="$(mktemp -d "$temporary_root/cascade-compatibility.XXXXXX")"
broker_log="$repository_root/target/compatibility-broker.log"
broker_pid=""

cleanup() {
  if [[ -n "$broker_pid" ]] && kill -0 "$broker_pid" 2>/dev/null; then
    kill -TERM "$broker_pid"
    wait "$broker_pid" || true
  fi
  if [[ -n "$broker_data" && -d "$broker_data" ]]; then
    case "$broker_data" in
      "$temporary_root"/cascade-compatibility.*) rm -rf -- "$broker_data" ;;
      *) echo "refusing to remove unexpected compatibility directory: $broker_data" >&2 ;;
    esac
  fi
}
trap cleanup EXIT

java -cp "target/docker-stage/lib/*" cascade.Main \
  --host 127.0.0.1 \
  --port 19092 \
  --advertised-host 127.0.0.1 \
  --advertised-port 19092 \
  --data-dir "$broker_data" \
  --flush-policy sync \
  --operations-port 19404 \
  >"$broker_log" 2>&1 &
broker_pid="$!"

ready=false
for _attempt in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:19404/ready >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$broker_pid" 2>/dev/null; then
    cat "$broker_log"
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  cat "$broker_log"
  exit 1
fi

export CASCADE_BOOTSTRAP_SERVERS=127.0.0.1:19092
npm ci --prefix compatibility/node
npm run smoke --prefix compatibility/node
python -m pip install --disable-pip-version-check --requirement compatibility/python/requirements.txt
python compatibility/python/smoke.py
(cd compatibility/go && go run .)
dotnet run --configuration Release --project compatibility/dotnet/CascadeCompatibility.csproj

if grep -q '"event":"protocol_error"' "$broker_log"; then
  cat "$broker_log"
  exit 1
fi
