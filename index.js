const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['kafka:9092','kafka2:9092','kafka3:9092'],
})

async function consumer() {
    const consumer = kafka.consumer({ groupId: 'test-group'})

    await consumer.connect()
    await consumer.subscribe({ topic: 'WordsWithCountsTopic', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
            })
        },
    })
}

consumer();