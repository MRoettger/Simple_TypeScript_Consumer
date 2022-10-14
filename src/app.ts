import * as ip from 'ip';
import { Kafka,logLevel } from 'kafkajs';

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  //logLevel: logLevel.DEBUG,
  //clientId: 'example-consumer',
  brokers: [`${host}:9093`],
  sasl: {
    mechanism: 'scram-sha-256',
    username: 'user',
    password: 'TqQ9WIpiM7',
  },
})

const topic = 'FLINK_TOPIC'
const consumer = kafka.consumer({ groupId: 'GROUP' })

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }:any) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })
}
console.log("-------------------------------------");
console.log("Welcome to the simple Kafka Consumer ");
console.log("-------------------------------------");
console.log("Host IP-Address: ",host);
console.log(" ");

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})