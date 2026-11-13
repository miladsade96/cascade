[CmdletBinding()]
param(
    [string]$BrokerImage = 'miladsade96/cascade:1.16.0',
    [string]$ExpectedVersion = '1.16.0',
    [ValidateRange(1, 1000000)][int]$Records = 1000,
    [string]$Report,
    [string]$MinioImage = 'minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e',
    [switch]$KeepResources
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$sbtRunner = Join-Path $repository $(if ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { 'sbt.bat' } else { 'sbt' })
if ([string]::IsNullOrWhiteSpace($Report)) {
    $Report = Join-Path $repository "artifacts/off-cluster-restore-$ExpectedVersion.json"
}
$Report = [IO.Path]::GetFullPath($Report)
if (Test-Path -LiteralPath $Report) { throw "Disaster-recovery evidence path already exists: $Report" }

function Invoke-Docker([string[]]$Arguments) {
    $output = @(& docker @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
    }
    return $output
}

function Get-FreePort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { return ([Net.IPEndPoint]$listener.LocalEndpoint).Port }
    finally { $listener.Stop() }
}

function Wait-Healthy([string]$Name) {
    for ($attempt = 0; $attempt -lt 180; $attempt++) {
        $status = (@(& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $Name 2>$null) -join '').Trim()
        if ($LASTEXITCODE -eq 0 -and $status -eq 'healthy') { return }
        if ($status -in @('exited', 'dead')) {
            throw "$Name exited before readiness:`n$((Invoke-Docker @('logs', $Name)) -join [Environment]::NewLine)"
        }
        Start-Sleep -Milliseconds 500
    }
    throw "$Name did not become healthy:`n$((Invoke-Docker @('logs', $Name)) -join [Environment]::NewLine)"
}

function Invoke-MinioClient([string]$Command, [string[]]$Mounts = @()) {
    $arguments = @('run', '--rm', '--pull', 'never', '--network', $networkName,
        '--tmpfs', '/tmp:size=32m,mode=1777,nosuid,nodev,noexec',
        '--env', "MINIO_ROOT_USER=$accessKey", '--env', "MINIO_ROOT_PASSWORD=$secretKey")
    foreach ($mount in $Mounts) { $arguments += @('--mount', $mount) }
    $arguments += @('--entrypoint', '/bin/sh', $MinioImage, '-ec', $Command)
    return Invoke-Docker $arguments
}

function Wait-ObjectStore {
    $command = '/usr/bin/mc alias set store http://object-store:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && /usr/bin/mc ready store >/dev/null'
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $output = @(& docker run --rm --pull never --network $networkName `
            --tmpfs '/tmp:size=32m,mode=1777,nosuid,nodev,noexec' `
            --env "MINIO_ROOT_USER=$accessKey" --env "MINIO_ROOT_PASSWORD=$secretKey" `
            --entrypoint /bin/sh $MinioImage -ec $command 2>&1)
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Object store did not become ready:`n$((Invoke-Docker @('logs', $objectStoreName)) -join [Environment]::NewLine)"
}

function Start-Broker([string]$Volume, [string]$DataPath) {
    Invoke-Docker @(
        'run', '--detach', '--init', '--name', $brokerName, '--read-only', '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges:true', '--memory', '2g',
        '--tmpfs', '/tmp:size=64m,mode=1777,nosuid,nodev,noexec',
        '--publish', "127.0.0.1:$brokerPort`:9092",
        '--mount', "type=volume,source=$Volume,target=/var/lib/cascade",
        $imageId, '--host', '0.0.0.0', '--port', '9092', '--advertised-host', '127.0.0.1',
        '--advertised-port', $brokerPort.ToString(), '--data-dir', $DataPath,
        '--flush-policy', 'sync', '--operations-port', '9404'
    ) | Out-Null
    Wait-Healthy $brokerName
}

function Invoke-Ledger([string]$Mode) {
    $command = "Test/runMain cascade.qualification.DisasterRecoveryLedger $Mode 127.0.0.1:$brokerPort $topicName $groupName $Records"
    $output = @(& $sbtRunner $command 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "Disaster-recovery ledger $Mode failed:`n$($output -join [Environment]::NewLine)" }
    $line = $output | Where-Object { $_ -match 'CASCADE_DR_RESULT\s+(\{.*\})' } | Select-Object -Last 1
    if ($null -eq $line -or $line -notmatch 'CASCADE_DR_RESULT\s+(\{.*\})') {
        throw "Disaster-recovery ledger $Mode did not emit machine-readable evidence."
    }
    return ($Matches[1] | ConvertFrom-Json)
}

function Write-AtomicJson([System.Collections.IDictionary]$Value) {
    $directory = Split-Path -Parent $Report
    if (-not (Test-Path -LiteralPath $directory)) { New-Item -ItemType Directory -Path $directory -Force | Out-Null }
    $temporary = "$Report.tmp"
    [IO.File]::WriteAllText($temporary, (($Value | ConvertTo-Json -Depth 8) + [Environment]::NewLine), [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $Report
}

$metadataOutput = Invoke-Docker @('image', 'inspect', $BrokerImage)
$metadata = @($metadataOutput | ConvertFrom-Json)[0]
if ($metadata.Config.User -ne '65532:65532') { throw 'Broker image must declare UID/GID 65532.' }
if ($metadata.Config.Labels.'org.opencontainers.image.version' -ne $ExpectedVersion) { throw 'Broker image version label is incorrect.' }
if ($metadata.Id -notmatch '^sha256:[0-9a-f]{64}$') { throw 'Broker image ID is not immutable.' }
$imageId = $metadata.Id
$registryDigest = @($metadata.RepoDigests | Where-Object { $_ -like "*@$imageId" } | Select-Object -First 1)
if ($registryDigest.Count -eq 0) { $registryDigest = @($metadata.RepoDigests | Select-Object -First 1) }

Invoke-Docker @('image', 'inspect', $MinioImage) | Out-Null
$runId = [Guid]::NewGuid().ToString('N').Substring(0, 12)
$prefix = "cascade-dr-$runId"
$networkName = "$prefix-network"
$brokerName = "$prefix-broker"
$objectStoreName = "$prefix-object-store"
$sourceVolume = "$prefix-source"
$stagingVolume = "$prefix-staging"
$restoredVolume = "$prefix-restored"
$objectVolume = "$prefix-object-data"
$brokerPort = Get-FreePort
$topicName = "dr-$runId"
$groupName = "dr-group-$runId"
$accessKey = "cascade$runId"
$secretKey = ([Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N'))
$startedAt = [DateTimeOffset]::UtcNow
$passed = $false
$phase = 'provision'
$evidence = [ordered]@{
    status = 'failed'
    phase = $phase
    started_at = $startedAt.ToString('O')
    image = $BrokerImage
    image_id = $imageId
    registry_digest = if ($registryDigest.Count -gt 0) { $registryDigest[0] } else { $null }
    release = $ExpectedVersion
    revision = $metadata.Config.Labels.'org.opencontainers.image.revision'
    records = $Records
}

try {
    Invoke-Docker @('network', 'create', '--label', 'cascade.qualification=off-cluster-restore', $networkName) | Out-Null
    foreach ($volume in @($sourceVolume, $stagingVolume, $restoredVolume, $objectVolume)) {
        Invoke-Docker @('volume', 'create', '--label', 'cascade.qualification=off-cluster-restore', $volume) | Out-Null
    }

    Invoke-Docker @(
        'run', '--detach', '--name', $objectStoreName, '--network', $networkName, '--network-alias', 'object-store',
        '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
        '--tmpfs', '/tmp:size=64m,mode=1777,nosuid,nodev,noexec',
        '--env', "MINIO_ROOT_USER=$accessKey", '--env', "MINIO_ROOT_PASSWORD=$secretKey",
        '--mount', "type=volume,source=$objectVolume,target=/data", $MinioImage,
        'server', '/data', '--address', ':9000', '--console-address', ':9001'
    ) | Out-Null
    Wait-ObjectStore

    # The first broker writes to a volume that is destroyed after object-store persistence.
    Start-Broker $sourceVolume '/var/lib/cascade'
    $phase = 'produce'
    $produced = Invoke-Ledger 'produce'
    Invoke-Docker @('stop', '--time', '120', $brokerName) | Out-Null
    Invoke-Docker @('rm', $brokerName) | Out-Null

    $phase = 'backup'
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $backupOutput = Invoke-Docker @(
        'run', '--rm', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
        '--mount', "type=volume,source=$sourceVolume,target=/source,readonly",
        '--mount', "type=volume,source=$stagingVolume,target=/var/lib/cascade",
        $imageId, 'backup', '--data-dir', '/source', '--backup-dir', '/var/lib/cascade/generation'
    )
    $timer.Stop()
    $backupMilliseconds = $timer.ElapsedMilliseconds
    $verifyBefore = Invoke-Docker @(
        'run', '--rm', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
        '--mount', "type=volume,source=$stagingVolume,target=/backup,readonly",
        $imageId, 'verify-backup', '--backup-dir', '/backup/generation'
    )

    $phase = 'upload'
    $timer.Restart()
    Invoke-MinioClient `
        '/usr/bin/mc alias set store http://object-store:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; /usr/bin/mc mb --ignore-existing store/cascade >/dev/null; /usr/bin/mc mirror --overwrite /backup/generation store/cascade/generation >/dev/null; /usr/bin/mc stat store/cascade/generation/cascade-backup.manifest >/dev/null' `
        @("type=volume,source=$stagingVolume,target=/backup,readonly") | Out-Null
    $timer.Stop()
    $uploadMilliseconds = $timer.ElapsedMilliseconds
    $durableAt = [DateTimeOffset]::UtcNow

    # Remove both local source and staging copies before recovery to prove object-store independence.
    foreach ($volume in @($sourceVolume, $stagingVolume)) {
        if ($volume -notlike "$prefix-*") { throw "Refusing unsafe qualification volume removal: $volume" }
        Invoke-Docker @('volume', 'rm', $volume) | Out-Null
    }
    Invoke-Docker @('volume', 'create', '--label', 'cascade.qualification=off-cluster-restore', $stagingVolume) | Out-Null

    $phase = 'recover'
    $rtoTimer = [Diagnostics.Stopwatch]::StartNew()
    $timer.Restart()
    Invoke-MinioClient `
        '/usr/bin/mc alias set store http://object-store:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; mkdir -p /backup/generation; /usr/bin/mc mirror --overwrite store/cascade/generation /backup/generation >/dev/null' `
        @("type=volume,source=$stagingVolume,target=/backup") | Out-Null
    $timer.Stop()
    $downloadMilliseconds = $timer.ElapsedMilliseconds
    $verifyAfter = Invoke-Docker @(
        'run', '--rm', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
        '--mount', "type=volume,source=$stagingVolume,target=/backup,readonly",
        $imageId, 'verify-backup', '--backup-dir', '/backup/generation'
    )

    $timer.Restart()
    Invoke-Docker @(
        'run', '--rm', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
        '--mount', "type=volume,source=$stagingVolume,target=/backup,readonly",
        '--mount', "type=volume,source=$restoredVolume,target=/var/lib/cascade",
        $imageId, 'restore', '--backup-dir', '/backup/generation', '--data-dir', '/var/lib/cascade/restored'
    ) | Out-Null
    $timer.Stop()
    $restoreMilliseconds = $timer.ElapsedMilliseconds

    $timer.Restart()
    Start-Broker $restoredVolume '/var/lib/cascade/restored'
    $timer.Stop()
    $brokerRecoveryMilliseconds = $timer.ElapsedMilliseconds
    $timer.Restart()
    $verified = Invoke-Ledger 'verify'
    $timer.Stop()
    $verificationMilliseconds = $timer.ElapsedMilliseconds
    $rtoTimer.Stop()

    if ($produced.records -ne $Records -or $verified.records -ne $Records) { throw 'Record count changed across recovery.' }
    if ($produced.last_offset -ne ($Records - 1) -or $verified.last_offset -ne ($Records - 1)) { throw 'Last offset changed across recovery.' }
    if ($produced.committed_offset -ne $Records -or $verified.committed_offset -ne $Records) { throw 'Committed offset changed across recovery.' }
    if ($produced.ledger_sha256 -ne $verified.ledger_sha256) { throw 'Ledger checksum changed across recovery.' }

    $phase = 'complete'
    $evidence = [ordered]@{
        status = 'passed'
        phase = $phase
        started_at = $startedAt.ToString('O')
        completed_at = [DateTimeOffset]::UtcNow.ToString('O')
        image = $BrokerImage
        image_id = $imageId
        registry_digest = if ($registryDigest.Count -gt 0) { $registryDigest[0] } else { $null }
        release = $ExpectedVersion
        revision = $metadata.Config.Labels.'org.opencontainers.image.revision'
        object_store = [ordered]@{
            kind = 'S3-compatible MinIO'
            image = $MinioImage
            outside_broker_cluster = $true
            independent_volume = $true
            physical_host_independent = $false
        }
        exactness = [ordered]@{
            acknowledged_records = [int]$produced.records
            recovered_records = [int]$verified.records
            record_loss = 0
            last_offset = [long]$verified.last_offset
            committed_offset = [long]$verified.committed_offset
            ledger_sha256 = [string]$verified.ledger_sha256
        }
        recovery_objective = [ordered]@{
            observed_rpo_records = 0
            observed_rpo_ms = 0
            protection_lag_ms = [Math]::Max(0, $durableAt.ToUnixTimeMilliseconds() - [long]$produced.last_ack_epoch_ms)
            rto_ms = $rtoTimer.ElapsedMilliseconds
        }
        timings_ms = [ordered]@{
            backup = $backupMilliseconds
            upload = $uploadMilliseconds
            download = $downloadMilliseconds
            restore = $restoreMilliseconds
            broker_ready = $brokerRecoveryMilliseconds
            exact_validation = $verificationMilliseconds
        }
        controls = [ordered]@{
            source_volume_destroyed_before_restore = $true
            local_staging_destroyed_before_restore = $true
            verified_before_upload = (($verifyBefore -join ' ') -match '^Verified| Verified')
            verified_after_download = (($verifyAfter -join ' ') -match '^Verified| Verified')
            synchronous_flush = $true
        }
        boundary = 'The object store is outside the broker cluster but shares this Docker host; this is not off-host or multi-terabyte evidence.'
    }
    Write-AtomicJson $evidence
    $passed = $true
    Write-Output "OFF_CLUSTER_RESTORE_RESULT passed image_id=$imageId records=$Records rpo_records=0 rto_ms=$($rtoTimer.ElapsedMilliseconds) report=$Report"
}
catch {
    $evidence['status'] = 'failed'
    $evidence['phase'] = $phase
    $evidence['completed_at'] = [DateTimeOffset]::UtcNow.ToString('O')
    $evidence['error'] = $_.Exception.Message
    Write-AtomicJson $evidence
    throw
}
finally {
    if ($KeepResources) {
        Write-Output "OFF_CLUSTER_RESTORE_RESOURCES prefix=$prefix"
    }
    else {
        foreach ($container in @($brokerName, $objectStoreName)) {
            if ($container -like "$prefix-*") { & docker rm --force $container 2>$null | Out-Null }
        }
        if ($networkName -like "$prefix-*") { & docker network rm $networkName 2>$null | Out-Null }
        foreach ($volume in @($sourceVolume, $stagingVolume, $restoredVolume, $objectVolume)) {
            if ($volume -like "$prefix-*") { & docker volume rm --force $volume 2>$null | Out-Null }
        }
    }
}

if (-not $passed) { exit 1 }
