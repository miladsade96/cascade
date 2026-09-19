[CmdletBinding()]
param(
    [string]$CascadeImage = 'miladsade96/cascade:1.8.0',
    [string]$KafkaImage = 'apache/kafka:4.3.1',
    [ValidateRange(1, 10000000)][int]$Records = 250000,
    [ValidateRange(0, 1000000)][int]$WarmupRecords = 25000,
    [ValidateRange(8, 1048576)][int]$PayloadBytes = 1024,
    [ValidateRange(1, 128)][int]$Partitions = 8,
    [ValidateRange(1, 64)][int]$Producers = 4,
    [string]$Java = 'java'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$classpathFile = Join-Path $repository 'target/streams/test/fullClasspath/_global/streams/export'
if (-not (Test-Path -LiteralPath $classpathFile)) { throw 'Run sbt Test/compile before the comparison.' }
$classpath = (Get-Content -LiteralPath $classpathFile -Raw).Trim()
$runId = [Guid]::NewGuid().ToString('N')
$artifactDirectory = Join-Path $repository "artifacts/comparison-$runId"
$cascadeName = "cascade-comparison-$runId"
$kafkaName = "kafka-comparison-$runId"
$cascadeVolume = "cascade-comparison-data-$runId"
$kafkaVolume = "kafka-comparison-data-$runId"

function Wait-Port([int]$Port) {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $client = [Net.Sockets.TcpClient]::new()
        try {
            $connection = $client.ConnectAsync('127.0.0.1', $Port)
            if ($connection.Wait(250) -and $client.Connected) { return }
        } catch {} finally { $client.Dispose() }
        Start-Sleep -Milliseconds 250
    }
    throw "Port $Port did not become available."
}

function Run-Benchmark([string]$Engine, [string]$Output) {
    & $Java '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' -cp $classpath cascade.performance.KafkaComparisonBenchmark `
        --engine $Engine --bootstrap '127.0.0.1:19092' --records $Records --warmup-records $WarmupRecords `
        --payload-bytes $PayloadBytes --partitions $Partitions --replication-factor 1 --producers $Producers `
        --compression lz4 --acks all --output $Output
    if ($LASTEXITCODE -ne 0) { throw "$Engine benchmark failed." }
}

try {
    New-Item -ItemType Directory -Path $artifactDirectory | Out-Null
    & docker volume create $cascadeVolume | Out-Null
    $cascadeStarted = [Diagnostics.Stopwatch]::StartNew()
    & docker run --detach --name $cascadeName --read-only --cap-drop ALL --security-opt no-new-privileges:true `
        --memory 4g --cpus 4 --tmpfs '/tmp:size=64m,mode=1777,nosuid,nodev,noexec' `
        --publish '127.0.0.1:19092:9092' --mount "type=volume,source=$cascadeVolume,target=/var/lib/cascade" `
        $CascadeImage --host 0.0.0.0 --port 9092 --advertised-host 127.0.0.1 --advertised-port 19092 `
        --data-dir /var/lib/cascade --flush-policy periodic --operations-port 9404 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Cascade comparison container failed to start.' }
    Wait-Port 19092
    $cascadeStarted.Stop()
    Run-Benchmark 'cascade' (Join-Path $artifactDirectory 'cascade.json')
    $cascadeStats = & docker stats --no-stream --format '{{.MemUsage}}|{{.CPUPerc}}' $cascadeName
    & docker stop --time 120 $cascadeName | Out-Null
    & docker rm $cascadeName | Out-Null

    & docker volume create $kafkaVolume | Out-Null
    $kafkaStarted = [Diagnostics.Stopwatch]::StartNew()
    & docker run --detach --name $kafkaName --memory 4g --cpus 4 --publish '127.0.0.1:19092:19092' `
        --mount "type=volume,source=$kafkaVolume,target=/tmp/kraft-combined-logs" `
        --env KAFKA_NODE_ID=1 --env 'KAFKA_PROCESS_ROLES=broker,controller' `
        --env 'KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093' `
        --env 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092' `
        --env 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' `
        --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
        --env 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093' `
        --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
        --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 `
        $KafkaImage | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Apache Kafka comparison container failed to start.' }
    Wait-Port 19092
    $kafkaStarted.Stop()
    Run-Benchmark 'apache-kafka' (Join-Path $artifactDirectory 'apache-kafka.json')
    $kafkaStats = & docker stats --no-stream --format '{{.MemUsage}}|{{.CPUPerc}}' $kafkaName

    Write-Output "COMPARISON_RESULT directory=$artifactDirectory"
    Write-Output "COMPARISON_SERVER engine=cascade startup_ms=$($cascadeStarted.ElapsedMilliseconds) stats=$cascadeStats"
    Write-Output "COMPARISON_SERVER engine=apache-kafka startup_ms=$($kafkaStarted.ElapsedMilliseconds) stats=$kafkaStats"
}
finally {
    foreach ($container in @($cascadeName, $kafkaName)) {
        if (& docker container ls --all --quiet --filter "name=^/$container`$") { & docker rm --force $container | Out-Null }
    }
    foreach ($volume in @($cascadeVolume, $kafkaVolume)) {
        if (& docker volume ls --quiet --filter "name=^$volume`$") { & docker volume rm $volume | Out-Null }
    }
}

