[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$EvidenceDirectory,
  [ValidateRange(1, 168)]
  [int]$DurationHours = 72,
  [ValidateRange(0, 604800)]
  [int]$DurationSeconds = 0,
  [ValidateRange(2, 1000)]
  [int]$Tenants = 8,
  [ValidateRange(1, 100000)]
  [int]$RecordsPerCycle = 100,
  [ValidateRange(32, 1048576)]
  [int]$PayloadBytes = 512,
  [ValidateRange(1, 86400)]
  [int]$ReportIntervalSeconds = 60,
  [ValidateRange(0, 86400)]
  [int]$RebalanceIntervalSeconds = 300,
  [ValidateRange(0, 86400)]
  [int]$ChurnIntervalSeconds = 900,
  [ValidateRange(0, 604800)]
  [int]$TlsRotationAfterSeconds = 3600,
  [ValidateRange(0, 100000)]
  [int]$AbortEveryCycles = 7,
  [ValidateRange(1024, 9223372036854775807)]
  [long]$QuotaBytesPerSecond = 16777216,
  [switch]$AllowDirtyTree,
  [switch]$KeepData
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$evidence = [System.IO.Path]::GetFullPath($EvidenceDirectory)
if (Test-Path -LiteralPath $evidence) {
  throw "Evidence directory already exists: $evidence"
}
[System.IO.Directory]::CreateDirectory($evidence) | Out-Null

$report = Join-Path $evidence "result.json"
$log = Join-Path $evidence "runner.log"
$environment = Join-Path $evidence "environment.json"
$data = Join-Path $evidence "broker-data"
$durationSeconds = if ($DurationSeconds -gt 0) { $DurationSeconds } else { [checked]($DurationHours * 3600) }

Push-Location $projectRoot
try {
  $revision = (& git rev-parse HEAD).Trim()
  if ($LASTEXITCODE -ne 0 -or $revision -notmatch '^[0-9a-f]{40}$') {
    throw "Unable to resolve the source revision"
  }
  $dirty = @(& git status --porcelain)
  if ($dirty.Count -gt 0 -and -not $AllowDirtyTree) {
    throw "The source tree is dirty. Commit the release candidate or pass -AllowDirtyTree for a non-release diagnostic run."
  }
  $previousErrorAction = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  $javaVersion = (& java -version 2>&1 | Out-String).Trim()
  $javaExitCode = $LASTEXITCODE
  $ErrorActionPreference = $previousErrorAction
  if ($javaExitCode -ne 0 -or $javaVersion -notmatch 'version "21[\.]') {
    throw "The authenticated soak requires Java 21"
  }

  $preflight = [ordered]@{
    status = "running"
    started_at = [DateTimeOffset]::UtcNow.ToString("o")
    source_revision = $revision
    dirty_tree = $dirty.Count -gt 0
    java = $javaVersion
    os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
    architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    logical_processors = [Environment]::ProcessorCount
    duration_hours = $DurationHours
    duration_seconds = $durationSeconds
    tenants = $Tenants
    records_per_cycle = $RecordsPerCycle
    payload_bytes = $PayloadBytes
    rebalance_interval_seconds = $RebalanceIntervalSeconds
    churn_interval_seconds = $ChurnIntervalSeconds
    tls_rotation_after_seconds = $TlsRotationAfterSeconds
    abort_every_cycles = $AbortEveryCycles
    quota_bytes_per_second = $QuotaBytesPerSecond
  }
  $preflight | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $environment -Encoding utf8

  $task = 'Test / runMain cascade.qualification.SoakTest' +
    ' --profile authenticated-cluster' +
    " --duration-seconds $durationSeconds" +
    " --tenants $Tenants" +
    " --records-per-cycle $RecordsPerCycle" +
    " --payload-bytes $PayloadBytes" +
    " --report-interval-seconds $ReportIntervalSeconds" +
    " --rebalance-interval-seconds $RebalanceIntervalSeconds" +
    " --churn-interval-seconds $ChurnIntervalSeconds" +
    " --tls-rotation-after-seconds $TlsRotationAfterSeconds" +
    " --abort-every-cycles $AbortEveryCycles" +
    " --quota-bytes-per-second $QuotaBytesPerSecond" +
    " --report `"$report`""
  if ($KeepData) { $task += " --data-dir `"$data`" --keep-data" }

  $sbt = if ($env:OS -eq "Windows_NT") { Join-Path $projectRoot "sbt.bat" } else { Join-Path $projectRoot "sbt" }
  $previousErrorAction = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  & $sbt $task 2>&1 | Tee-Object -LiteralPath $log
  $soakExitCode = $LASTEXITCODE
  $ErrorActionPreference = $previousErrorAction
  if ($soakExitCode -ne 0) {
    throw "Authenticated soak process failed with exit code $soakExitCode"
  }
  if (-not (Test-Path -LiteralPath $report)) {
    throw "Authenticated soak did not produce result.json"
  }

  $result = Get-Content -Raw -LiteralPath $report | ConvertFrom-Json
  if ($result.status -ne "passed") { throw "Authenticated soak result is not passed" }
  if ($result.profile -ne "authenticated-cluster") { throw "Unexpected soak profile: $($result.profile)" }
  if ([long]$result.configured_duration_seconds -ne $durationSeconds) { throw "Soak duration evidence does not match the request" }
  if ([long]$result.records -le 0 -or [long]$result.verified_records -ne [long]$result.records) {
    throw "Exact record verification did not pass"
  }
  if ([long]$result.mismatches -ne 0) { throw "Soak reported record or offset mismatches" }
  if ([long]$result.quota_reservations -le 0) { throw "Distributed quotas were not exercised" }
  if ([long]$result.quota_rejected -ne 0) { throw "Distributed quotas rejected workload requests" }
  if ([long]$result.authentication_successes -le 0) { throw "Authentication was not exercised" }
  if ([long]$result.authentication_failures -ne 0) { throw "Authentication failures were recorded" }
  if ([int]$result.authenticated_tenants -ne $Tenants) { throw "Not every tenant principal was audited" }
  if (-not [bool]$result.restart_verified) { throw "Final full-cluster restart was not verified" }
  if ($RebalanceIntervalSeconds -gt 0 -and $durationSeconds -ge $RebalanceIntervalSeconds -and [long]$result.rebalances -le 0) {
    throw "Consumer rebalance evidence is missing"
  }
  if ($ChurnIntervalSeconds -gt 0 -and $durationSeconds -ge $ChurnIntervalSeconds -and [long]$result.controller_failovers -le 0) {
    throw "Controller failover evidence is missing"
  }
  if ($TlsRotationAfterSeconds -gt 0 -and $durationSeconds -ge $TlsRotationAfterSeconds -and [long]$result.tls_rotations -le 0) {
    throw "TLS rotation evidence is missing"
  }

  $preflight.status = "passed"
  $preflight.completed_at = [DateTimeOffset]::UtcNow.ToString("o")
  $preflight.records = [long]$result.records
  $preflight.cycles = [long]$result.cycles
  $preflight.controller_failovers = [long]$result.controller_failovers
  $preflight.rebalances = [long]$result.rebalances
  $preflight.tls_rotations = [long]$result.tls_rotations
  $preflight | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $environment -Encoding utf8
  Write-Host "AUTHENTICATED_SOAK_PASSED evidence=$evidence records=$($result.records)"
}
catch {
  $failure = $_.Exception.Message
  try {
    $failed = [ordered]@{
      status = "failed"
      completed_at = [DateTimeOffset]::UtcNow.ToString("o")
      failure = $failure
      evidence_directory = $evidence
    }
    $failed | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $environment -Encoding utf8
  } catch {
    Write-Warning "Unable to update failure evidence: $($_.Exception.Message)"
  }
  throw
}
finally {
  Pop-Location
}
