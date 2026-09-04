const { Kafka, logLevel } = require('kafkajs')

const bootstrap = process.env.CASCADE_BOOTSTRAP_SERVERS || '127.0.0.1:19092'
const topic = process.argv[2] || `node-compat-${Date.now()}`
const replicationFactor = Number(process.env.CASCADE_REPLICATION_FACTOR || '1')
if (!Number.isInteger(replicationFactor) || replicationFactor < 1 || replicationFactor > 32767) {
  throw new Error('CASCADE_REPLICATION_FACTOR must be an integer from 1 to 32767')
}
const expected = Array.from({ length: 25 }, (_, index) => `node-${index}`)

async function main() {
  const kafka = new Kafka({
    clientId: 'cascade-kafkajs-compatibility',
    brokers: bootstrap.split(','),
    connectionTimeout: 5000,
    requestTimeout: 15000,
    logLevel: logLevel.NOTHING
  })
  const admin = kafka.admin()
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
  const consumer = kafka.consumer({ groupId: `${topic}-readers` })

  await admin.connect()
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic, numPartitions: 1, replicationFactor }]
    })
  } finally {
    await admin.disconnect()
  }

  await producer.connect()
  try {
    await producer.send({
      topic,
      acks: -1,
      messages: expected.map(value => ({ value }))
    })
  } finally {
    await producer.disconnect()
  }

  const actual = []
  await consumer.connect()
  try {
    await consumer.subscribe({ topic, fromBeginning: true })
    await new Promise(async (resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error(`KafkaJS received only ${actual.length}/${expected.length} records`)), 20000)
      try {
        await consumer.run({
          eachMessage: async ({ message }) => {
            actual.push(message.value.toString('utf8'))
            if (actual.length === expected.length) {
              clearTimeout(timeout)
              resolve()
            }
          }
        })
      } catch (error) {
        clearTimeout(timeout)
        reject(error)
      }
    })
    await consumer.commitOffsets([{ topic, partition: 0, offset: String(expected.length) }])
  } finally {
    await consumer.disconnect()
  }

  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`KafkaJS mismatch: expected=${JSON.stringify(expected)}, actual=${JSON.stringify(actual)}`)
  }
  await admin.connect()
  try {
    const offsets = await admin.fetchOffsets({ groupId: `${topic}-readers`, topics: [topic] })
    const committed = offsets.find(item => item.topic === topic)?.partitions.find(item => item.partition === 0)?.offset
    if (committed !== String(expected.length)) {
      throw new Error(`KafkaJS committed offset mismatch: expected=${expected.length}, actual=${committed}`)
    }
  } finally {
    await admin.disconnect()
  }
  console.log(`KafkaJS verified ${actual.length}/${expected.length} records through ${bootstrap}`)
}

main().catch(error => {
  console.error(error)
  process.exitCode = 1
})
