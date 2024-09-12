const { Kafka } = require('kafkajs');
const fs = require('fs');
const lz4 = require('lz4');
const { v4: uuidv4 } = require('uuid');

const kafka = new Kafka({
    clientId: 'whatever-name',
    brokers: ['rpk0.bitquery.io:A', 'rpk1.bitquery.io:B', 'rpk2.bitquery.io:C'],
    ssl: {
        rejectUnauthorized: false,
        ca: [fs.readFileSync('server.cer.pem', 'utf-8')],
        key: fs.readFileSync('client.key.pem', 'utf-8'),
        cert: fs.readFileSync('client.cer.pem', 'utf-8')
    },
});

const uniqueGroupId = `divyasshree-test-${uuidv4()}`;
console.log(`Generated Group ID: ${uniqueGroupId}`);

const topic = 'tron.broadcasted.transactions';
const consumer = kafka.consumer({ groupId: uniqueGroupId, sessionTimeout: 30000 });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
            try {
                const buffer = message.value;
                let decompressedValue;

                // Try to decompress LZ4 frame
                try {
                    decompressedValue = lz4.decode(buffer).toString('utf-8');
                } catch (err) {
                    console.error('LZ4 frame decompression failed:', err);
                    decompressedValue = buffer.toString('utf-8');
                }

                // Log message data
                const logEntry = {
                    partition,
                    offset: message.offset,
                    value: decompressedValue
                };
                console.log(logEntry);

            } catch (err) {
                console.error('Error processing message:', err);
            }
        },
    });
};

run().catch(console.error);
