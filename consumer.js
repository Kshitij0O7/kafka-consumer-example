const { Kafka } = require('kafkajs');
const fs = require('fs');
const { CompressionTypes, CompressionCodecs } = require('kafkajs')
const LZ4 = require('kafkajs-lz4')

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec
// pre-requisites

const username = '<USERNAME>'
const password = '<PASSWORD>'
const topic = 'tron.broadcasted.transactions'

// end of pre-requisites

const kafka = new Kafka({
    clientId: username,
    brokers: ['rpk0.bitquery.io:9093', 'rpk1.bitquery.io:9093', 'rpk2.bitquery.io:9093'],
    ssl: {
        rejectUnauthorized: false,
        ca: [fs.readFileSync('server.cer.pem', 'utf-8')],
        key: fs.readFileSync('client.key.pem', 'utf-8'),
        cert: fs.readFileSync('client.cer.pem', 'utf-8')
    },
    sasl: {
        mechanism: "scram-sha-512",
        username: username,
        password: password
    }

});

const consumer = kafka.consumer({ groupId: username+'my-group', sessionTimeout: 30000 });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
            try {
                const buffer = message.value;

                // Log message data
                const logEntry = {
                    partition,
                    offset: message.offset,
                    value: buffer.toString('utf-8')
                };
                console.log(logEntry);

            } catch (err) {
                console.error('Error processing message:', err);
            }
        },
    });
};

run().catch(console.error);
