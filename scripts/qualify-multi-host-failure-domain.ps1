[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][ValidateCount(3, 3)][string[]]$Contexts,
    [Parameter(Mandatory = $true)][ValidateCount(3, 3)][string[]]$Hosts,
    [string]$BaselineImage = 'miladsade96/cascade:1.15.0',
    [string]$BaselineVersion = '1.15.0',
    [string]$CandidateImage = 'miladsade96/cascade:1.16.0',
    [string]$CandidateVersion = '1.16.0',
    [ValidateRange(1024, 65535)][int]$Port = 9092,
    [ValidateRange(10, 10000)][int]$RecordsPerPhase = 100,
    [string]$Report,
    [switch]$KeepResources
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$sbtRunner = Join-Path $repository $(if ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { 'sbt.bat' } else { 'sbt' })
if ([string]::IsNullOrWhiteSpace($Report)) {
    $Report = Join-Path $repository "artifacts/multi-host-$BaselineVersion-to-$CandidateVersion.json"
}
$Report = [IO.Path]::GetFullPath($Report)
if (Test-Path -LiteralPath $Report) { throw "Multi-host evidence path already exists: $Report" }
if (@($Hosts | Select-Object -Unique).Count -ne 3) { throw 'Three distinct routable host names or addresses are required.' }
foreach ($hostName in $Hosts) {
    if ($hostName -in @('localhost', '127.0.0.1', '::1') -or $hostName -notmatch '^[a-zA-Z0-9.-]+$') {
        throw "Multi-host endpoint must be a non-loopback DNS name or IP address: $hostName"
    }
}

function Invoke-Docker([string]$Context, [string[]]$Arguments, [switch]$AllowFailure) {
    $output = @(& docker --context $Context @Arguments 2>&1)
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0 -and -not $AllowFailure) {
        throw "docker --context $Context $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
    }
    return [pscustomobject]@{ ExitCode = $exitCode; Output = $output }
}

function Write-AtomicJson([System.Collections.IDictionary]$Value) {
    $directory = Split-Path -Parent $Report
    if (-not (Test-Path -LiteralPath $directory)) { New-Item -ItemType Directory -Path $directory -Force | Out-Null }
    $temporary = "$Report.tmp"
    [IO.File]::WriteAllText($temporary, (($Value | ConvertTo-Json -Depth 10) + [Environment]::NewLine), [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $Report
}

function Container-Name([int]$NodeId) { return "$prefix-node-$NodeId" }
function Volume-Name([int]$NodeId) { return "$prefix-data-$NodeId" }

function Wait-NodeReady([pscustomobject]$Node) {
    $name = Container-Name $Node.Id
    for ($attempt = 0; $attempt -lt 180; $attempt++) {
        $result = Invoke-Docker $Node.Context @(
            'inspect', '--format', '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}', $name
        ) -AllowFailure
        $status = ($result.Output -join '').Trim()
        if ($result.ExitCode -eq 0 -and $status -eq 'healthy') { return }
        if ($status -in @('exited', 'dead')) {
            $logs = Invoke-Docker $Node.Context @('logs', $name) -AllowFailure
            throw "node $($Node.Id) exited before readiness:`n$($logs.Output -join [Environment]::NewLine)"
        }
        Start-Sleep -Milliseconds 500
    }
    throw "node $($Node.Id) did not become healthy"
}

function Start-Node([pscustomobject]$Node, [string]$ImageId) {
    $result = Invoke-Docker $Node.Context @(
        'run', '--detach', '--init', '--name', (Container-Name $Node.Id),
        '--label', 'cascade.qualification=multi-host-failure-domain',
        '--network', 'bridge', '--read-only', '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges:true', '--memory', '2g',
        '--tmpfs', '/tmp:size=64m,mode=1777,nosuid,nodev,noexec',
        '--publish', "0.0.0.0:$Port`:9092",
        '--mount', "type=volume,source=$(Volume-Name $Node.Id),target=/var/lib/cascade",
        $ImageId,
        '--host', '0.0.0.0', '--port', '9092', '--advertised-host', $Node.Host,
        '--advertised-port', $Port.ToString(), '--node-id', $Node.Id.ToString(),
        '--data-dir', '/var/lib/cascade', '--cluster-nodes', $clusterNodes,
        '--advertised-cluster-nodes', $clusterNodes, '--controller-id', '1',
        '--default-replication-factor', '3', '--min-insync-replicas', '2',
        '--peer-timeout-ms', '1000', '--controller-heartbeat-ms', '200',
        '--controller-election-timeout-ms', '2000', '--flush-policy', 'sync',
        '--operations-host', '127.0.0.1', '--operations-port', '9404'
    )
    $result.Output | Out-Null
}

function Replace-Node([pscustomobject]$Node, [string]$ImageId) {
    $name = Container-Name $Node.Id
    Invoke-Docker $Node.Context @('stop', '--time', '120', $name) | Out-Null
    Invoke-Docker $Node.Context @('rm', $name) | Out-Null
    Start-Node $Node $ImageId
    Wait-NodeReady $Node
}

function Invoke-Ledger([string[]]$Arguments) {
    $command = "Test/runMain cascade.qualification.MultiHostLedger $($Arguments -join ' ')"
    $output = @(& $sbtRunner $command 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "Multi-host ledger failed:`n$($output -join [Environment]::NewLine)" }
    $line = $output | Where-Object { $_ -match 'CASCADE_MULTI_HOST_RESULT\s+(\{.*\})' } | Select-Object -Last 1
    if ($null -eq $line -or $line -notmatch 'CASCADE_MULTI_HOST_RESULT\s+(\{.*\})') {
        throw 'Multi-host ledger did not emit machine-readable evidence.'
    }
    return ($Matches[1] | ConvertFrom-Json)
}

function Stop-NodesTogether([pscustomobject[]]$Targets) {
    $dockerPath = (Get-Command docker).Source
    $processes = foreach ($node in $Targets) {
        $start = [Diagnostics.ProcessStartInfo]::new()
        $start.FileName = $dockerPath
        $start.UseShellExecute = $false
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        foreach ($argument in @('--context', $node.Context, 'stop', '--time', '120', (Container-Name $node.Id))) {
            [void]$start.ArgumentList.Add($argument)
        }
        $process = [Diagnostics.Process]::new()
        $process.StartInfo = $start
        if (-not $process.Start()) { throw "could not start simultaneous stop for node $($node.Id)" }
        [pscustomobject]@{ Node = $node; Process = $process }
    }
    foreach ($entry in $processes) {
        $entry.Process.WaitForExit()
        $stdout = $entry.Process.StandardOutput.ReadToEnd()
        $stderr = $entry.Process.StandardError.ReadToEnd()
        if ($entry.Process.ExitCode -ne 0) {
            throw "simultaneous stop failed for node $($entry.Node.Id): $stdout $stderr"
        }
        $entry.Process.Dispose()
    }
}

$runId = [Guid]::NewGuid().ToString('N').Substring(0, 12)
$prefix = "cascade-multihost-$runId"
$startedAt = [DateTimeOffset]::UtcNow
$phase = 'preflight'
$nodes = @()
$evidence = [ordered]@{
    status = 'failed'
    phase = $phase
    started_at = $startedAt.ToString('O')
    baseline_image = $BaselineImage
    candidate_image = $CandidateImage
    contexts = $Contexts
    hosts = $Hosts
}

try {
    for ($index = 0; $index -lt 3; $index++) {
        $context = $Contexts[$index]
        $infoResult = Invoke-Docker $context @('info', '--format', '{{json .}}')
        $info = ($infoResult.Output -join [Environment]::NewLine) | ConvertFrom-Json
        if ($info.OSType -ne 'linux' -or $info.Architecture -ne 'x86_64') {
            throw "context $context must target a Linux x86_64 engine"
        }
        $nodes += [pscustomobject]@{
            Id = $index + 1
            Context = $context
            Host = $Hosts[$index]
            EngineId = [string]$info.ID
            EngineName = [string]$info.Name
            ServerVersion = [string]$info.ServerVersion
        }
    }
    if (@($nodes.EngineId | Select-Object -Unique).Count -ne 3) {
        throw "Three independent Docker engine IDs are required; observed $((($nodes.EngineId | Select-Object -Unique) -join ', '))"
    }

    foreach ($node in $nodes) {
        foreach ($image in @($BaselineImage, $CandidateImage)) {
            $available = Invoke-Docker $node.Context @('image', 'inspect', $image) -AllowFailure
            if ($available.ExitCode -ne 0) { Invoke-Docker $node.Context @('pull', $image) | Out-Null }
        }
        $baselineMetadata = ((Invoke-Docker $node.Context @('image', 'inspect', $BaselineImage)).Output | ConvertFrom-Json)[0]
        $candidateMetadata = ((Invoke-Docker $node.Context @('image', 'inspect', $CandidateImage)).Output | ConvertFrom-Json)[0]
        if ($baselineMetadata.Config.Labels.'org.opencontainers.image.version' -ne $BaselineVersion) {
            throw "baseline version label is invalid on context $($node.Context)"
        }
        if ($candidateMetadata.Config.Labels.'org.opencontainers.image.version' -ne $CandidateVersion) {
            throw "candidate version label is invalid on context $($node.Context)"
        }
        if ($baselineMetadata.Config.User -ne '65532:65532' -or $candidateMetadata.Config.User -ne '65532:65532') {
            throw "release images must use UID/GID 65532 on context $($node.Context)"
        }
        $node | Add-Member -NotePropertyName BaselineId -NotePropertyValue ([string]$baselineMetadata.Id)
        $node | Add-Member -NotePropertyName CandidateId -NotePropertyValue ([string]$candidateMetadata.Id)
    }
    if (@($nodes.BaselineId | Select-Object -Unique).Count -ne 1 -or @($nodes.CandidateId | Select-Object -Unique).Count -ne 1) {
        throw 'Every host must resolve each release tag to the same immutable image ID.'
    }

    $clusterNodes = ($nodes | ForEach-Object { "$($_.Id)@$($_.Host):$Port" }) -join ','
    $bootstrap = ($nodes | ForEach-Object { "$($_.Host):$Port" }) -join ','
    $topic = "multi-host-$runId"
    $probeTopic = "multi-host-probe-$runId"

    $phase = 'provision'
    foreach ($node in $nodes) {
        Invoke-Docker $node.Context @('volume', 'create', '--label', 'cascade.qualification=multi-host-failure-domain', (Volume-Name $node.Id)) | Out-Null
        Start-Node $node $node.BaselineId
    }
    foreach ($node in $nodes) { Wait-NodeReady $node }

    $phase = 'baseline'
    Invoke-Ledger @('produce', $bootstrap, $topic, '0', $RecordsPerPhase.ToString(), 'true') | Out-Null
    Invoke-Ledger @('produce', $bootstrap, $probeTopic, '0', '1', 'true') | Out-Null
    Invoke-Ledger @('await-isr', $bootstrap, $topic, '3') | Out-Null

    $phase = 'network-partition'
    $partitionTimer = [Diagnostics.Stopwatch]::StartNew()
    $partitioned = $nodes[2]
    Invoke-Docker $partitioned.Context @('network', 'disconnect', 'bridge', (Container-Name $partitioned.Id)) | Out-Null
    Invoke-Ledger @('await-isr', ($nodes[0..1] | ForEach-Object { "$($_.Host):$Port" }) -join ',', $topic, '2') | Out-Null
    Invoke-Ledger @('produce', ($nodes[0..1] | ForEach-Object { "$($_.Host):$Port" }) -join ',', $topic,
        $RecordsPerPhase.ToString(), $RecordsPerPhase.ToString(), 'false') | Out-Null
    Invoke-Docker $partitioned.Context @('network', 'connect', 'bridge', (Container-Name $partitioned.Id)) | Out-Null
    Wait-NodeReady $partitioned
    Invoke-Ledger @('await-isr', $bootstrap, $topic, '3') | Out-Null
    $partitionTimer.Stop()

    $phase = 'simultaneous-controller-broker-loss'
    $controller = Invoke-Ledger @('controller', $bootstrap)
    $controllerNode = $nodes | Where-Object Id -eq ([int]$controller.controller_id)
    if ($null -eq $controllerNode) { throw "reported controller $($controller.controller_id) is not a configured node" }
    $secondFailed = $nodes | Where-Object Id -ne $controllerNode.Id | Select-Object -First 1
    $survivor = $nodes | Where-Object { $_.Id -ne $controllerNode.Id -and $_.Id -ne $secondFailed.Id }
    $lossTimer = [Diagnostics.Stopwatch]::StartNew()
    Stop-NodesTogether @($controllerNode, $secondFailed)
    Invoke-Ledger @('expect-unavailable', "$($survivor.Host):$Port", $probeTopic) | Out-Null
    Invoke-Docker $secondFailed.Context @('start', (Container-Name $secondFailed.Id)) | Out-Null
    Wait-NodeReady $secondFailed
    $majorityBootstrap = "$($survivor.Host):$Port,$($secondFailed.Host):$Port"
    Invoke-Ledger @('produce', $majorityBootstrap, $topic, (2 * $RecordsPerPhase).ToString(), $RecordsPerPhase.ToString(), 'false') | Out-Null
    Invoke-Docker $controllerNode.Context @('start', (Container-Name $controllerNode.Id)) | Out-Null
    Wait-NodeReady $controllerNode
    Invoke-Ledger @('await-isr', $bootstrap, $topic, '3') | Out-Null
    $lossTimer.Stop()
    $preUpgradeCount = 3 * $RecordsPerPhase
    $preUpgradeVerify = Invoke-Ledger @('verify', $bootstrap, $topic, $preUpgradeCount.ToString())

    $phase = 'rolling-upgrade'
    $upgradeDurations = [ordered]@{}
    $nextOffset = $preUpgradeCount
    foreach ($node in $nodes) {
        $upgradeTimer = [Diagnostics.Stopwatch]::StartNew()
        Replace-Node $node $node.CandidateId
        Invoke-Ledger @('await-isr', $bootstrap, $topic, '3') | Out-Null
        Invoke-Ledger @('produce', $bootstrap, $topic, $nextOffset.ToString(), $RecordsPerPhase.ToString(), 'false') | Out-Null
        $nextOffset += $RecordsPerPhase
        $upgradeTimer.Stop()
        $upgradeDurations["node_$($node.Id)"] = $upgradeTimer.ElapsedMilliseconds
    }
    $postUpgradeVerify = Invoke-Ledger @('verify', $bootstrap, $topic, $nextOffset.ToString())

    $phase = 'full-restart'
    Stop-NodesTogether $nodes
    foreach ($node in $nodes) { Invoke-Docker $node.Context @('start', (Container-Name $node.Id)) | Out-Null }
    foreach ($node in $nodes) { Wait-NodeReady $node }
    Invoke-Ledger @('await-isr', $bootstrap, $topic, '3') | Out-Null
    $restartVerify = Invoke-Ledger @('verify', $bootstrap, $topic, $nextOffset.ToString())
    if ([long]$restartVerify.previous_committed_offset -ne $nextOffset) {
        throw "committed offset did not survive the full multi-host restart"
    }
    foreach ($node in $nodes) {
        $logs = Invoke-Docker $node.Context @('logs', (Container-Name $node.Id))
        if (($logs.Output -join [Environment]::NewLine) -match '"event":"protocol_error"') {
            throw "node $($node.Id) reported a protocol error"
        }
    }

    $phase = 'complete'
    $evidence = [ordered]@{
        status = 'passed'
        phase = $phase
        started_at = $startedAt.ToString('O')
        completed_at = [DateTimeOffset]::UtcNow.ToString('O')
        baseline_image = $BaselineImage
        baseline_version = $BaselineVersion
        baseline_image_id = $nodes[0].BaselineId
        candidate_image = $CandidateImage
        candidate_version = $CandidateVersion
        candidate_image_id = $nodes[0].CandidateId
        nodes = @($nodes | ForEach-Object { [ordered]@{
            id = $_.Id; context = $_.Context; host = $_.Host; engine_id = $_.EngineId
            engine_name = $_.EngineName; server_version = $_.ServerVersion
        }})
        failure_domains = [ordered]@{
            distinct_engine_ids = 3
            directional_container_network_partition = $true
            simultaneous_controller_and_broker_loss = $true
            controller_id_at_failure = [int]$controller.controller_id
            second_failed_broker_id = [int]$secondFailed.Id
            surviving_broker_id = [int]$survivor.Id
            minority_write_acknowledged = $false
        }
        exactness = [ordered]@{
            produced_records = $nextOffset
            recovered_records = [int]$restartVerify.records
            record_loss = 0
            last_offset = [long]$restartVerify.last_offset
            committed_offset = [long]$restartVerify.committed_offset
            ledger_sha256 = [string]$restartVerify.ledger_sha256
        }
        timings_ms = [ordered]@{
            partition_and_recovery = $partitionTimer.ElapsedMilliseconds
            simultaneous_loss_and_recovery = $lossTimer.ElapsedMilliseconds
            rolling_upgrade = $upgradeDurations
        }
        checks = [ordered]@{
            pre_upgrade_records = [int]$preUpgradeVerify.records
            post_upgrade_records = [int]$postUpgradeVerify.records
            full_restart_previous_committed_offset = [long]$restartVerify.previous_committed_offset
            protocol_errors = 0
        }
    }
    Write-AtomicJson $evidence
    Write-Output "MULTI_HOST_RESULT passed engines=3 records=$nextOffset controller_loss=$($controller.controller_id) report=$Report"
}
catch {
    $evidence['status'] = 'failed'
    $evidence['phase'] = $phase
    $evidence['completed_at'] = [DateTimeOffset]::UtcNow.ToString('O')
    $evidence['observed_nodes'] = @($nodes | ForEach-Object { [ordered]@{
        id = $_.Id; context = $_.Context; host = $_.Host; engine_id = $_.EngineId; engine_name = $_.EngineName
    }})
    $evidence['error'] = $_.Exception.Message
    Write-AtomicJson $evidence
    throw
}
finally {
    if ($KeepResources) {
        Write-Output "MULTI_HOST_RESOURCES prefix=$prefix"
    }
    else {
        foreach ($node in $nodes) {
            $name = Container-Name $node.Id
            $volume = Volume-Name $node.Id
            if ($name -like "$prefix-*") { Invoke-Docker $node.Context @('rm', '--force', $name) -AllowFailure | Out-Null }
            if ($volume -like "$prefix-*") { Invoke-Docker $node.Context @('volume', 'rm', '--force', $volume) -AllowFailure | Out-Null }
        }
    }
}
