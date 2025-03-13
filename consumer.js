const { Kafka } = require('kafkajs');
const { Worker } = require('worker_threads');
const fs = require("fs");
const { CompressionTypes, CompressionCodecs } = require("kafkajs");
const LZ4 = require("kafkajs-lz4");

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;

// Pre-requisites
const username = "trontest1";
const password = "9ijSSnbrj7lldsqq1taSl3YOdujRWB";
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

// Create worker threads
for (let i = 0; i < NUM_WORKERS; i++) {
    workerPool.push(new Worker('./worker.js'));
}

// Function to get an available worker
const getAvailableWorker = () => workerPool[Math.floor(Math.random() * workerPool.length)];

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
            const worker = getAvailableWorker();

            worker.postMessage({
                partition,
                offset: message.offset,
                value: message.value.toString('utf-8'),
            });

            worker.once('message', (response) => {
                if (response.status === 'done') {
                    console.log(`Message processed, offset: ${response.offset}`);
                } else {
                    console.error(`Processing failed: ${response.error}`);
                }
            });
        },
    });
};

run().catch(console.error);
