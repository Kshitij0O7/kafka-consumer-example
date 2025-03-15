const { Kafka } = require('kafkajs');
const { Worker } = require('worker_threads');
const fs = require("fs");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const LZ4 = require("kafkajs-lz4");
require('dotenv').config();

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;

// Pre-requisites
const username = process.env.USERNAME;
const password = process.env.PASSWORD;
const topic = "tron.broadcasted.transactions";
// End of pre-requisites

const kafka = new Kafka({
  clientId: username,
  brokers: [
    "rpk0.bitquery.io:9093",
    "rpk1.bitquery.io:9093",
    "rpk2.bitquery.io:9093",
  ],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync("server.cer.pem", "utf-8")],
    key: fs.readFileSync("client.key.pem", "utf-8"),
    cert: fs.readFileSync("client.cer.pem", "utf-8"),
  },
  sasl: {
    mechanism: "scram-sha-512",
    username: username,
    password: password,
  },
});

const consumer = kafka.consumer({
    groupId: username + "-my-group",
    sessionTimeout: 30000,
});

// Number of worker threads (based on CPU cores)
const NUM_WORKERS = 4;
const workerPool = [];
let workerIndex = 0;

// Create worker threads
for (let i = 0; i < NUM_WORKERS; i++) {
    workerPool.push(new Worker('./worker.js'));
}

// Function to get the next available worker in a round-robin manner
const getNextWorker = () => {
    const worker = workerPool[workerIndex]; // Get the worker at the current index
    workerIndex = (workerIndex + 1) % NUM_WORKERS; // Move to the next worker
    return worker;
};

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
            const worker = getNextWorker();

            worker.postMessage({
                partition,
                offset: message.offset,
                value: message.value.toString('utf-8'),
            });

            worker.once('message', (response) => {
                if (response.status === 'done') {
                    console.log(`Message processed, offset: ${response.offset}, partition: ${partition}`);
                } else {
                    console.error(`Processing failed: ${response.error}`);
                }
            });
        },
    });
};

run().catch(console.error);
