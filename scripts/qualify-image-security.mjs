import { createHash } from 'node:crypto'
import { spawnSync } from 'node:child_process'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

const indexType = 'application/vnd.oci.image.index.v1+json'
const manifestType = 'application/vnd.oci.image.manifest.v1+json'
const spdxType = 'https://spdx.dev/Document'
const digestPattern = /^sha256:[a-f0-9]{64}$/
const requireThat = (condition, message) => { if (!condition) throw new Error(message) }
export const digest = bytes => `sha256:${createHash('sha256').update(bytes).digest('hex')}`

export function checkedJson(bytes, descriptor) {
  requireThat(digestPattern.test(descriptor.digest), 'Invalid SHA-256 descriptor')
  requireThat(bytes.length === descriptor.size && digest(bytes) === descriptor.digest, 'OCI blob size/digest mismatch')
  return JSON.parse(bytes.toString('utf8'))
}

export function validateSbom(statement, runtimeDigest, version) {
  requireThat(statement._type === 'https://in-toto.io/Statement/v1' && statement.predicateType === spdxType, 'Missing SPDX attestation')
  requireThat(statement.subject?.length === 1 && `sha256:${statement.subject[0].digest?.sha256}` === runtimeDigest, 'SBOM subject mismatch')
  const packages = statement.predicate?.packages
  requireThat(statement.predicate?.spdxVersion === 'SPDX-2.3' && Array.isArray(packages), 'Invalid SPDX inventory')
  for (const name of ['base-files', 'ca-certificates', 'libc6', 'openjdk', 'scala-library', 'scala3-library_3']) {
    requireThat(packages.some(pkg => pkg.name === name && typeof pkg.versionInfo === 'string' && pkg.versionInfo.length > 0), `Incomplete SBOM: ${name}`)
  }
  requireThat(packages.some(pkg => pkg.name === 'cascade_3' && pkg.versionInfo === version), 'SBOM application version mismatch')
  requireThat(!packages.some(pkg => /^(libssl|libcrypto|openssl)(\b|\d)/i.test(pkg.name)), 'Unexpected OpenSSL package in no-OpenSSL runtime')
  return packages.length
}

export function validateReport(report, scannerExit) {
  requireThat(scannerExit === 0, `Scanner failed or found vulnerabilities (exit ${scannerExit})`)
  requireThat(report.version === '2.1.0' && report.runs?.length === 1, 'Missing or ambiguous SARIF run')
  const run = report.runs[0]
  requireThat(run.tool?.driver?.name === 'docker scout' && typeof run.tool.driver.version === 'string', 'Unexpected scanner identity')
  requireThat(Array.isArray(run.results) && Array.isArray(run.tool.driver.rules), 'Incomplete SARIF report')
  requireThat(run.results.length === 0 && run.tool.driver.rules.length === 0, 'Vulnerability findings block publication at every severity')
  requireThat((run.invocations ?? []).every(invocation => invocation.executionSuccessful === true &&
    !(invocation.toolExecutionNotifications ?? []).some(event => event.level === 'error') &&
    !(invocation.toolConfigurationNotifications ?? []).some(event => event.level === 'error')), 'Scanner execution/configuration errors')
  return run.tool.driver.version
}

// Read only named archive members to stdout; never extract paths supplied by an archive.
export function inspectArchive(readMember, imageId, version) {
  requireThat(digestPattern.test(imageId), 'Expected an immutable OCI image ID')
  const outer = JSON.parse(readMember('index.json').toString('utf8'))
  requireThat(outer.schemaVersion === 2 && outer.manifests?.length === 1, 'Ambiguous image archive')
  const descriptor = outer.manifests[0]
  requireThat(descriptor.mediaType === indexType && descriptor.digest === imageId, 'Archive does not match inspected image; attestations require the containerd image store')
  const read = descriptor => {
    requireThat(digestPattern.test(descriptor.digest), 'Invalid archive blob path')
    return checkedJson(readMember(`blobs/sha256/${descriptor.digest.slice(7)}`), descriptor)
  }
  const index = read(descriptor)
  requireThat(index.schemaVersion === 2 && index.mediaType === indexType && index.manifests?.length === 2, 'Expected exactly one runtime and one attestation manifest')
  const runtimes = index.manifests.filter(item => item.platform?.os === 'linux' && item.platform?.architecture === 'amd64')
  const attestations = index.manifests.filter(item => item.annotations?.['vnd.docker.reference.type'] === 'attestation-manifest')
  requireThat(runtimes.length === 1 && attestations.length === 1 && runtimes[0] !== attestations[0], 'Only the qualified Linux/amd64 platform may be published')
  const runtimeDescriptor = runtimes[0]
  requireThat(runtimeDescriptor.mediaType === manifestType, 'Invalid runtime manifest type')
  const runtime = read(runtimeDescriptor)
  const config = read(runtime.config)
  requireThat(config.os === 'linux' && config.architecture === 'amd64' && config.config?.User === '65532:65532', 'Runtime platform/user mismatch')
  requireThat(config.config?.Labels?.['org.opencontainers.image.version'] === version, 'Runtime label version mismatch')
  requireThat(attestations[0].annotations['vnd.docker.reference.digest'] === runtimeDescriptor.digest, 'Attestation reference mismatch')
  const attestation = read(attestations[0])
  const statements = attestation.layers.map(layer => ({ layer, statement: read(layer) }))
  const sboms = statements.filter(item => item.layer.annotations?.['in-toto.io/predicate-type'] === spdxType)
  const provenance = statements.filter(item => item.layer.annotations?.['in-toto.io/predicate-type'] === 'https://slsa.dev/provenance/v1')
  requireThat(sboms.length === 1 && provenance.length === 1, 'Missing or ambiguous SBOM/provenance')
  const proof = provenance[0].statement
  requireThat(proof.predicateType === 'https://slsa.dev/provenance/v1' && proof.subject?.length === 1 &&
    `sha256:${proof.subject[0].digest?.sha256}` === runtimeDescriptor.digest, 'Provenance subject mismatch')
  const statement = sboms[0].statement
  const packageCount = validateSbom(statement, runtimeDescriptor.digest, version)
  return { statement, packageCount, runtimeDigest: runtimeDescriptor.digest }
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: 'buffer', maxBuffer: 32 * 1024 * 1024, timeout: 600000, ...options })
  if (result.error) throw result.error
  requireThat(result.status === 0, `${command} failed (exit ${result.status}): ${result.stderr?.toString().slice(-4000)}`)
  return result.stdout
}

export function qualify(image, version, outputDirectory) {
  requireThat(typeof image === 'string' && image.length > 0 && !image.startsWith('-'), 'Invalid local image reference')
  requireThat(/^\d+\.\d+\.\d+$/.test(version), 'Expected a release version')
  const output = resolve(outputDirectory)
  mkdirSync(dirname(output), { recursive: true })
  mkdirSync(output) // A fresh directory prevents a failed scan from reusing an older passing report.
  const metadata = JSON.parse(run('docker', ['image', 'inspect', image]))[0]
  const imageId = metadata.Id
  const archive = resolve(output, 'image.tar')
  run('docker', ['save', '--output', archive, imageId])
  const entries = run('tar', ['-tf', archive]).toString('utf8').split(/\r?\n/)
  const inventory = inspectArchive(member => {
    requireThat(entries.filter(entry => entry === member).length === 1, `Missing/duplicate archive member: ${member}`)
    return run('tar', ['-xOf', archive, member])
  }, imageId, version)
  const sbom = resolve(output, 'sbom.intoto.json')
  const reportPath = resolve(output, 'vulnerabilities.sarif.json')
  writeFileSync(sbom, JSON.stringify(inventory.statement))
  const scan = spawnSync('docker', ['scout', 'cves', `sbom://${sbom}`, '--exit-code', '--format', 'sarif', '--output', reportPath],
    { encoding: 'utf8', timeout: 600000, maxBuffer: 16 * 1024 * 1024 })
  writeFileSync(resolve(output, 'scanner.log'), (scan.stdout ?? '') + (scan.stderr ?? '') + (scan.error?.message ?? ''))
  requireThat(!scan.error && scan.status === 0, `Security scan blocked publication (exit ${scan.status}); see ${output}`)
  const scannerVersion = validateReport(JSON.parse(readFileSync(reportPath, 'utf8')), scan.status)
  const current = JSON.parse(run('docker', ['image', 'inspect', image]))[0]
  requireThat(current.Id === imageId, 'Local image tag changed during qualification')
  const evidence = { status: 'passed', imageId, runtimeDigest: inventory.runtimeDigest, version,
    platform: 'linux/amd64', packages: inventory.packageCount, vulnerabilities: 0, scannerVersion, scannedAt: new Date().toISOString() }
  writeFileSync(resolve(output, 'result.json'), JSON.stringify(evidence, null, 2))
  console.log(JSON.stringify(evidence))
  return evidence
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    requireThat(process.argv.length === 5, 'Usage: node scripts/qualify-image-security.mjs <local-image> <version> <new-evidence-directory>')
    qualify(...process.argv.slice(2))
  } catch (error) {
    console.error(`IMAGE_SECURITY_GATE_FAILED: ${error.message}`)
    process.exitCode = 1
  }
}
