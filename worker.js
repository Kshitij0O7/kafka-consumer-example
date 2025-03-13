const { parentPort } = require('worker_threads');

parentPort.on('message', async (message) => {
    try {
        // Simulate processing (e.g., saving to DB, transforming data)
        console.log(`Worker processing message from partition ${message.partition}:`, message.value);
        
        // Simulated delay (remove or optimize as needed)
        // await new Promise(resolve => setTimeout(resolve, 50));

        // Notify main thread when done
        parentPort.postMessage({ status: 'done', offset: message.offset });
    } catch (err) {
        console.error('Worker Error:', err);
        parentPort.postMessage({ status: 'error', error: err.message });
    }
});
