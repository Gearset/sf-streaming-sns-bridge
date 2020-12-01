const log = require('lambda-log');
const Listener = require('./listener').Listener;
const Publisher = require('./publisher').Publisher;
const ReplayStore = require('./replay-store').ReplayStore;

class Bridge {
    constructor(config) {
        this.workers = {};
        this.config = config;
    }

    status() {
        return Object.entries(this.workers).reduce((result, entry) => {
            const [key, worker] = entry;
            result[key] = worker.buildStatusDTO();
            return result;
        }, {});
    }

    loadConfig() {
        return this.config.get();
    }

    reload() {
        return this.loadConfig()
            .then(config => {
                const replayStore = new ReplayStore(options);
                const newWorkers = {};
                const options = config.options;
                log.options.debug = options && options.debug;
                for (let [envName, envDetails] of Object.entries(config).filter(([key, value]) => key !== 'options')) {
                    const sfConnOptions = envDetails.connection;
                    envDetails.channels.forEach(mappingConfig => {
                        const publisher = new Publisher(mappingConfig.snsTopicArn);
                        const channelKey = `${envName}//${mappingConfig.channelName}`;
                        newWorkers[channelKey] = new Listener(log, replayStore, publisher, channelKey, sfConnOptions, mappingConfig.channelName, options);
                    });
                }
                log.info(`Loaded configuration`, {channels: Object.keys(newWorkers)});
                return newWorkers;
            })
            .then(newWorkers => this.stopAll().then(() => newWorkers))
            .then(newWorkers => {
                this.workers = newWorkers;
                return this.startAll();
            });
    }

    startAll() {
        return this.doAll((key, worker) => 
            worker.start().catch(e => log.error(e, {description: `[${key}] Failed to start`})));
    }

    stopAll() {
        return this.doAll((key, worker) => 
            worker.stop().catch(e => log.error(e, {description: `[${key}] Failed to stop`})));
    }

    doAll(func) {
        return Promise.all(Object.entries(this.workers).map(entry => {
            const [key, worker] = entry;
            return func(key, worker);
        }));
    }
}

module.exports = { Bridge };