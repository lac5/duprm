const { parentPort, workerData } = require('worker_threads');
const fs = require('fs/promises');
const nodeID3 = require('node-id3');
const getMd5 = require('md5');

async function getBuffer(file) {
    let buffer = await fs.readFile(file);
    return nodeID3.removeTagsFromBuffer(buffer);
}

async function getData(file) {
    let time = (await fs.stat(file)).birthtimeMs;
    let md5 = getMd5(await getBuffer(file));
    return { time, md5 };
}

parentPort.on('message', (file) => {
    getData(file)
        .then((data) => parentPort.postMessage({ file, data }))
        .catch(error => parentPort.postMessage({ file, error }));
});
