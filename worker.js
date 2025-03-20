const { parentPort } = require('worker_threads');

parentPort.on('message', async (message) => {
    try {
        let hash = message.value[0].TransactionHeader.Hash;

        // Notify main thread when done
        parentPort.postMessage({ status: 'done', time: Date.now(), hash: hash });
    } catch (err) {
        console.error('Worker Error:', err);
        parentPort.postMessage({ status: 'error', error: err.message });
    }
});
