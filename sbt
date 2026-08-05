#!/usr/bin/env sh
set -eu
SBT_VERSION=1.12.6
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
LAUNCHER_DIR="$ROOT/.tools"
LAUNCHER="$LAUNCHER_DIR/sbt-launch-$SBT_VERSION.jar"
if [ ! -f "$LAUNCHER" ]; then
  mkdir -p "$LAUNCHER_DIR"
  echo "Downloading sbt $SBT_VERSION launcher..."
  curl --fail --location --silent --show-error --output "$LAUNCHER" \
    "https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/$SBT_VERSION/sbt-launch-$SBT_VERSION.jar"
fi
exec java -Xms256m -Xmx2g -Dsbt.supershell=false -Dsbt.log.noformat=true -jar "$LAUNCHER" "$@"
