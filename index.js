#!/usr/bin/env node
const { Worker } = require('worker_threads');
const path = require('path');
const { createReadStream } = require('fs');
const glob = require('fast-glob');
const colors = require('colors/safe');
const trashImport = import('trash');

const workerFile = path.join(__dirname, 'worker.js');

async function* readLines(file) {
    let previous = '';
    let eol = /[\r\n]/;
    for await (const chunk of createReadStream(file)) {
        previous += chunk;
        let eolIndex;
        while ((eolIndex = previous.search(eol)) >= 0) {
            // line includes the EOL
            const line = previous.slice(0, eolIndex);
            if (line.length > 0) {
                yield line;
            }
            previous = previous.slice(eolIndex + 1);
        }
    }
    if (previous.length > 0) {
        yield previous;
    }
}

class Mp3BachWorker extends Worker {
    constructor() {
        super(workerFile);
        this.callbacks = new Map();
        this.on('message', ({ file, error, data }) => {
            let callback = this.callbacks.get(file);
            if (callback) {
                callback(error, data);
                this.callbacks.delete(file);
            } else {
                console.error('Unknown file: %s', file);
            }
        });
        this.on('exit', (code) => {
            for (let [file, callback] of this.callbacks) {
                callback(new Error(`Worker stopped with exit code ${code}`));
                this.callbacks.delete(file);
            }
        });
    }

    getData(file) {
        return new Promise((resolve, reject) => {
            this.callbacks.set(file, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    postFile(file) {
        this.postMessage(file);
    }
}

function nextWorker(workers) {
    for (let i = 1; i < workers.length; i++) {
        if (workers[0].callbacks.size > workers[i].callbacks.size) {
            [workers[0], workers[i]] = [workers[i], workers[0]];
            return workers[i];
        }
    }
    return workers[0];
}

exports.duprm = duprm;
/**
 * @param {{ dir: string, list?: string }} options
 * @returns {Promise<void>}
 */
async function duprm(options) {
    const trash = (await trashImport).default;
    let filesFound = 0;
    let foundAll = false;
    let filesTrashed = 0;
    let files = [];
    let dots = '...';
    function logProgress() {
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        process.stdout.write(colors.gray(`keep: ${files.length} / trash: ${filesTrashed} / total: ${filesFound + (foundAll ? '' : ' (scanning)')} ${dots}`));
        if (dots.length >= 3) {
            dots = '';
        } else {
            dots += '.';
        }
    }
    logProgress();
    let progressInterval = setInterval(logProgress, 1000);
    async function doTask(filename, worker) {
        let name, time, md5;
        try {
            name = path.join(options.dir, filename);
            ({ time, md5 } = await worker.getData(name));
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            console.log('%s %s', colors.yellow(md5), colors.green(name));
            let deleted = false;
            let trashP = [];
            for (let i = files.length - 1; i >= 0; i--) {
                let file = files[i];
                if (md5 === file.md5) {
                    process.stdout.clearLine();
                    process.stdout.cursorTo(0);
                    console.log('motch (%s):\n> %s \n> %s', colors.yellow(md5), colors.green(name), colors.blue(file.name));
                    // if file1 is newer than file2:
                    if (time > file.time) {
                        process.stdout.clearLine();
                        process.stdout.cursorTo(0);
                        console.log('trash:\n> %s', colors.blue(file.name));
                        filesTrashed++;
                        trashP.push(trash(file.name));
                        files.splice(i, 1);
                    } else {
                        deleted = true;
                    }
                    break;
                }
            }
            if (deleted) {
                process.stdout.clearLine();
                process.stdout.cursorTo(0);
                console.log('trash:\n> %s', colors.green(name));
                filesTrashed++;
                trashP.push(trash(name));
            } else {
                files.push({ name, time, md5 });
            }
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            logProgress();
            await Promise.all(trashP);
        } catch (e) {
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            console.error(colors.red('ERROR:'));
            console.error(e);
            logProgress();
        }
    }
    let workers = Array(64).fill().map(() => new Mp3BachWorker());
    let tasks = [];
    for await (let value of (options.list ?
        readLines(options.list) :
        glob.stream('**/*.mp3', {
            cwd: options.dir,
        }))
    ) {
        filesFound++;
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        logProgress();
        let filename = String(value);
        let worker = nextWorker(workers);
        worker.postFile(filename);
        tasks.push(doTask(filename, worker));
    }
    foundAll = true;
    await Promise.all(tasks);
    clearInterval(progressInterval);
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    dots = '...';
    logProgress();
    console.log(' done');
}

if (require.main === module) {
    const argv = require('yargs')
        .command('$0 [dir]', 'Find and remove duplicate MP3 files. This will delete older duplicates so only the newest ones are left.', yargs => yargs
            .positional('dir', {
                type: 'string',
                default: '.',
                description: 'search directory'
            })
            .option('list', {
                type: 'string',
                default: '',
                description: 'list of files to process instead of scanning'
            })
        )
        .version()
        .help()
        .argv;

    duprm(argv).catch(console.error);
}
