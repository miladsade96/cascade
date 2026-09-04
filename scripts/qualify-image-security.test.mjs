import test from 'node:test'
import assert from 'node:assert/strict'
import { checkedJson, digest, inspectArchive, validateReport, validateSbom } from './qualify-image-security.mjs'

const version = '1.3.1'
const runtimeDigest = `sha256:${'a'.repeat(64)}`
const clean = () => ({ version: '2.1.0', runs: [{ tool: { driver: { name: 'docker scout', version: '1.24.0', rules: [] } }, results: [] }] })
const statement = (subject = runtimeDigest) => ({
  _type: 'https://in-toto.io/Statement/v1', predicateType: 'https://spdx.dev/Document',
  subject: [{ digest: { sha256: subject.slice(7) } }],
  predicate: { spdxVersion: 'SPDX-2.3', packages: ['alpine-baselayout-data', 'ca-certificates-bundle', 'musl', 'libstdc++', 'openjdk', 'scala-library', 'scala3-library_3', 'cascade_3']
    .map(name => ({ name, versionInfo: name === 'cascade_3' ? version : '1.0' })) }
})

test('accepts a complete clean Scout report', () => assert.equal(validateReport(clean(), 0), '1.24.0'))
for (const exit of [1, 2, 127, null]) {
  test(`scanner exit ${exit} cannot pass even with a stale clean report`, () => assert.throws(() => validateReport(clean(), exit)))
}
for (const severity of ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'UNSPECIFIED', 'NEW-SEVERITY']) {
  test(`blocks ${severity} findings without severity filters or exceptions`, () => {
    const report = clean()
    report.runs[0].results.push({ ruleId: 'CVE-test', level: 'none', properties: { severity }, suppressions: [{ status: 'accepted' }] })
    assert.throws(() => validateReport(report, 0))
  })
}
for (const [name, change] of [
  ['missing results', report => delete report.runs[0].results],
  ['missing rules', report => delete report.runs[0].tool.driver.rules],
  ['unreported rule', report => report.runs[0].tool.driver.rules.push({ id: 'CVE-test' })],
  ['multiple runs', report => report.runs.push(structuredClone(report.runs[0]))],
  ['wrong scanner', report => { report.runs[0].tool.driver.name = 'fake' }],
  ['failed invocation', report => { report.runs[0].invocations = [{ executionSuccessful: false }] }],
  ['execution notification', report => { report.runs[0].invocations = [{ executionSuccessful: true, toolExecutionNotifications: [{ level: 'error' }] }] }],
  ['configuration notification', report => { report.runs[0].invocations = [{ executionSuccessful: true, toolConfigurationNotifications: [{ level: 'error' }] }] }]
]) {
  test(`rejects ${name}`, () => { const report = clean(); change(report); assert.throws(() => validateReport(report, 0)) })
}
test('rejects malformed SARIF roots', () => {
  for (const report of [{}, { version: '2.1.0', runs: [] }, { version: '1', runs: clean().runs }]) assert.throws(() => validateReport(report, 0))
})
test('requires the complete runtime inventory', () => {
  assert.equal(validateSbom(statement(), runtimeDigest, version), 8)
  for (const name of statement().predicate.packages.map(pkg => pkg.name)) {
    const sbom = statement()
    sbom.predicate.packages = sbom.predicate.packages.filter(pkg => pkg.name !== name)
    assert.throws(() => validateSbom(sbom, runtimeDigest, version))
  }
})
test('rejects mismatched SBOM subject and version', () => {
  assert.throws(() => validateSbom(statement(), `sha256:${'b'.repeat(64)}`, version))
  assert.throws(() => validateSbom(statement(), runtimeDigest, '1.3.0'))
})
test('rejects OpenSSL, glibc, shell and package-manager reintroduction', () => {
  for (const name of ['libssl3', 'libssl3t64', 'libcrypto3', 'openssl', 'libc6', 'glibc', 'busybox', 'apk-tools']) {
    const sbom = statement()
    sbom.predicate.packages.push({ name, versionInfo: '3.0' })
    assert.throws(() => validateSbom(sbom, runtimeDigest, version))
  }
})
test('checks blob bytes, size and digest syntax', () => {
  const bytes = Buffer.from('{}')
  assert.deepEqual(checkedJson(bytes, { digest: digest(bytes), size: 2 }), {})
  assert.throws(() => checkedJson(bytes, { digest: digest(bytes), size: 3 }))
  assert.throws(() => checkedJson(bytes, { digest: runtimeDigest, size: 2 }))
  assert.throws(() => checkedJson(bytes, { digest: '../escape', size: 2 }))
})

function archiveFixture(mutate = () => {}) {
  const files = new Map()
  const add = value => {
    const bytes = Buffer.from(JSON.stringify(value))
    const descriptor = { digest: digest(bytes), size: bytes.length, mediaType: value.mediaType }
    files.set(`blobs/sha256/${descriptor.digest.slice(7)}`, bytes)
    return descriptor
  }
  const config = { os: 'linux', architecture: 'amd64', config: { User: '65532:65532', Labels: { 'org.opencontainers.image.version': version } } }
  mutate('config', config)
  const runtime = add({ schemaVersion: 2, mediaType: 'application/vnd.oci.image.manifest.v1+json', config: add(config), layers: [] })
  runtime.platform = { os: 'linux', architecture: 'amd64' }
  mutate('runtime', runtime)
  const sbom = statement(runtime.digest)
  mutate('sbom', sbom)
  const proof = { ...statement(runtime.digest), predicateType: 'https://slsa.dev/provenance/v1' }
  mutate('proof', proof)
  const layers = [sbom, proof].map(value => ({ ...add(value), annotations: { 'in-toto.io/predicate-type': value.predicateType } }))
  const attestation = add({ schemaVersion: 2, layers })
  attestation.platform = { os: 'unknown', architecture: 'unknown' }
  attestation.annotations = { 'vnd.docker.reference.type': 'attestation-manifest', 'vnd.docker.reference.digest': runtime.digest }
  const index = { schemaVersion: 2, mediaType: 'application/vnd.oci.image.index.v1+json', manifests: [runtime, attestation] }
  mutate('index', index)
  const descriptor = add(index)
  files.set('index.json', Buffer.from(JSON.stringify({ schemaVersion: 2, manifests: [descriptor] })))
  return { files, id: descriptor.digest }
}
test('binds the archive SBOM to the inspected index, platform, config and runtime', () => {
  const fixture = archiveFixture()
  assert.equal(inspectArchive(member => fixture.files.get(member), fixture.id, version).packageCount, 8)
  assert.throws(() => inspectArchive(member => fixture.files.get(member), runtimeDigest, version))
})
for (const [name, target, change] of [
  ['root runtime', 'config', value => { value.config.User = '0' }],
  ['wrong release', 'config', value => { value.config.Labels['org.opencontainers.image.version'] = '1.3.0' }],
  ['unqualified platform', 'runtime', value => { value.platform.architecture = 'arm64' }],
  ['missing attestation', 'index', value => value.manifests.pop()],
  ['extra platform', 'index', value => value.manifests.push(structuredClone(value.manifests[0]))],
  ['empty inventory', 'sbom', value => { value.predicate.packages = [] }],
  ['wrong provenance subject', 'proof', value => { value.subject[0].digest.sha256 = 'f'.repeat(64) }]
]) {
  test(`archive rejects ${name}`, () => {
    const fixture = archiveFixture((kind, value) => { if (kind === target) change(value) })
    assert.throws(() => inspectArchive(member => fixture.files.get(member), fixture.id, version))
  })
}
