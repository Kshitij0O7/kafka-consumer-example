const { Kafka } = require('kafkajs');
const fs = require('fs');
const { CompressionTypes, CompressionCodecs } = require('kafkajs');
const LZ4 = require('kafkajs-lz4');
const { writeToExcel } = require('./helpers/writeToExel');
const { getAverage } = require('./helpers/average');
require('dotenv').config();

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec
// pre-requisites

const username = process.env.USERNAME;
const password = process.env.PASSWORD;
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

// Excel File Handling
const FILE_PATH = 'output1.xlsx';
const SHEET_NAME = 'Messages';

const writeQueue = []; // Store messages to be written

const write = async () => await writeToExcel(FILE_PATH, SHEET_NAME, writeQueue);
// Run file writing at intervals to prevent congestion
setInterval(write, 1); // Every 1 milli second

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    let sum, count = 0;

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
            try {
                const buffer = message.value;

                // Log message data
                const logEntry = {
                    partition,
                    offset: message.offset,
                    value: JSON.parse(buffer.toString('utf-8'))
                };
                let time = Date.now();
                let hash = logEntry.value[0].TransactionHeader.Hash;
                writeQueue.push([hash, time]);
                console.log("Entry sent");
            } catch (err) {
                console.error('Error processing message:', err);
            }
        },
    }).finally(console.log(getAverage(FILE_PATH, SHEET_NAME)));
};

run().catch(console.error);