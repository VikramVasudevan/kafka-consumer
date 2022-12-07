const { Kafka } = require('kafkajs')
const sql = require('mssql')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['kafka:9092', 'kafka2:9092', 'kafka3:9092'],
})

const sqlConfig = {
    port: 1433,
    server: 'mssql',
    user: 'demouser',
    password: 'demouser123',
    database: 'demo',
    stream: false,
    options: {
        trustedConnection: true,
        encrypt: true,
        enableArithAbort: true,
        trustServerCertificate: true,

    },
}

async function consumer() {
    await sql.connect(sqlConfig)
    const ps = new sql.PreparedStatement()
    ps.input('message', sql.VarChar(sql.MAX))
    await ps.prepare('INSERT INTO messages(message) VALUES(@message)');


    const consumer = kafka.consumer({ groupId: 'test-group' })

    await consumer.connect()
    await consumer.subscribe({ topic: 'WordsWithCountsTopic', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
            });
            try {
                await ps.execute(
                    { message: message.value.toString() },
                )
            } catch (e) {
                console.error("Error loading into mssql", e);
            }
        },
    })
}

consumer();