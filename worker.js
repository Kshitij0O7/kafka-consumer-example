const { parentPort } = require('worker_threads');

parentPort.on('message', async (message) => {
    try {
        let t1 = message.value[0].TransactionHeader.Time;
        let t2 = message.value[0].TransactionHeader.Timestamp;
        let lag = (t1 - t2)/10e9;

        // Notify main thread when done
        parentPort.postMessage({ status: 'done', partition: message.partition, offset: message.offset, lag: lag });
    } catch (err) {
        console.error('Worker Error:', err);
        parentPort.postMessage({ status: 'error', error: err.message });
    }
});
